pub mod raft {
    extern crate raft as tikv_raft;

    use memmap::MmapMut;
    use protobuf::{parse_from_bytes, Message as PbMessage};
    use std::fs::remove_dir_all;
    use std::io::prelude::*;
    use std::io::{Error as IOError, ErrorKind::NotFound};
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{
        convert::TryInto, fs::create_dir_all, fs::File, fs::OpenOptions, io::SeekFrom, io::Write,
        mem::size_of, ops::Range, path::PathBuf,
    };
    use tikv_raft::{prelude::*, storage::*, util::limit_size, Error, StorageError};

    pub trait RaftStorage: Storage {
        fn append_log(&mut self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), Error>;
        fn set_conf_state(
            &mut self,
            cs: ConfState,
            pending_membership_change: Option<(ConfState, u64)>,
        );
        fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error>;
        fn new_with_conf_state(dir: Option<&str>, conf_state: (Vec<u64>, Vec<u64>)) -> Self;
        fn clear(&mut self) -> Result<(), IOError>;
    }

    impl RaftStorage for MemStorage {
        fn append_log(&mut self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), Error> {
            self.wl().append(entries)
        }

        fn set_conf_state(
            &mut self,
            cs: ConfState,
            pending_membership_change: Option<(ConfState, u64)>,
        ) {
            self.wl().set_conf_state(cs, pending_membership_change);
        }

        fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
            self.wl().mut_hard_state().commit = commit;
            self.wl().mut_hard_state().term = term;
            Ok(())
        }

        fn new_with_conf_state(_dir: Option<&str>, conf_state: (Vec<u64>, Vec<u64>)) -> Self {
            MemStorage::new_with_conf_state(conf_state)
        }

        fn clear(&mut self) -> Result<(), IOError> {
            Ok(())
        }
    }

    struct FileMmap {
        file: File,
        mem_map: MmapMut,
    }

    impl FileMmap {
        fn new(file: File, mem_map: MmapMut) -> FileMmap {
            FileMmap { file, mem_map }
        }
    }

    #[derive(Clone)]
    pub struct DiskStorage {
        core: Arc<RwLock<DiskStorageCore>>,
    }

    impl DiskStorage {
        // Opens up a read lock on the storage and returns a guard handle. Use this
        // with functions that don't require mutation.
        fn rl(&self) -> RwLockReadGuard<'_, DiskStorageCore> {
            self.core.read().unwrap()
        }

        // Opens up a write lock on the storage and returns guard handle. Use this
        // with functions that take a mutable reference to self.
        fn wl(&self) -> RwLockWriteGuard<'_, DiskStorageCore> {
            self.core.write().unwrap()
        }

        #[allow(dead_code)]
        pub fn new(dir: &str) -> DiskStorage {
            let core = Arc::new(RwLock::new(DiskStorageCore::new(dir)));
            DiskStorage { core }
        }

        pub fn new_with_conf_state(dir: &str, conf_state: (Vec<u64>, Vec<u64>)) -> DiskStorage {
            let core = Arc::new(RwLock::new(DiskStorageCore::new_with_conf_state(
                dir, conf_state,
            )));
            DiskStorage { core }
        }
    }

    impl RaftStorage for DiskStorage {
        fn append_log(&mut self, entries: &[Entry]) -> Result<(), Error> {
            self.wl().append_log(entries)
        }

        fn set_conf_state(
            &mut self,
            cs: ConfState,
            pending_membership_change: Option<(ConfState, u64)>,
        ) {
            self.wl().set_conf_state(cs, pending_membership_change);
        }

        fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
            self.wl().set_hard_state(commit, term)
        }

        fn new_with_conf_state(dir: Option<&str>, conf_state: (Vec<u64>, Vec<u64>)) -> Self {
            DiskStorage::new_with_conf_state(dir.expect("No DiskStorage path provided"), conf_state)
        }

        fn clear(&mut self) -> Result<(), IOError> {
            self.wl().clear_dir()
        }
    }

    impl Storage for DiskStorage {
        fn initial_state(&self) -> Result<RaftState, Error> {
            self.rl().initial_state()
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
        ) -> Result<Vec<Entry>, Error> {
            self.wl().entries(low, high, max_size)
        }

        fn term(&self, idx: u64) -> Result<u64, Error> {
            self.rl().term(idx)
        }

        fn first_index(&self) -> Result<u64, Error> {
            self.rl().first_index()
        }

        fn last_index(&self) -> Result<u64, Error> {
            self.rl().last_index()
        }

        fn snapshot(&self, _request_index: u64) -> Result<Snapshot, Error> {
            unimplemented!()
        }
    }

    struct DiskStorageCore {
        dir: String,
        pending_conf_state: Option<ConfState>,
        pending_conf_state_start_index: Option<u64>,
        conf_state: ConfState,
        hard_state: MmapMut,
        log: FileMmap,
        offset: FileMmap,       // file that maps from index to byte offset
        raft_metadata: MmapMut, // memory map with metadata of raft index of first and last entry in log
        // TODO: Persist these as well?
        num_entries: u64,
        snapshot_metadata: SnapshotMetadata,
    }

    impl DiskStorageCore {
        const TERM_INDEX: Range<usize> = 0..8;
        const VOTE_INDEX: Range<usize> = 8..16;
        const COMMIT_INDEX: Range<usize> = 16..24;

        const FIRST_INDEX_IS_SET: Range<usize> = 0..1; // 1 if first_index is set
        const FIRST_INDEX: Range<usize> = 1..9;
        const LAST_INDEX_IS_SET: Range<usize> = 9..10; // 1 if last_index is set
        const LAST_INDEX: Range<usize> = 10..18;

        const FILE_SIZE: u64 = 20971520;

        fn new(dir: &str) -> DiskStorageCore {
            create_dir_all(dir)
                .unwrap_or_else(|_| panic!("Failed to create given directory: {}", dir));

            let log_path = PathBuf::from(format!("{}/log", dir));
            let log_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&log_path)
                .expect("Failed to create/open log file");
            log_file
                .set_len(DiskStorageCore::FILE_SIZE)
                .expect("Failed to set file length of log"); // LogCabin also uses 8MB files...
            let log_mmap = unsafe {
                MmapMut::map_mut(&log_file).expect("Failed to create memory map for log")
            };
            let log = FileMmap::new(log_file, log_mmap);

            let hs_path = PathBuf::from(format!("{}/hard_state", dir));
            let hs_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&hs_path)
                .expect("Failed to create/open hard_state file");
            hs_file
                .set_len(3 * size_of::<u64>() as u64)
                .expect("Failed to set file length of hard_state"); // we only need 3 u64: term, vote and commit
            let hard_state = unsafe {
                MmapMut::map_mut(&hs_file).expect("Failed to create memory map for hard_state")
            };

            let offset_path = PathBuf::from(format!("{}/offset", dir));
            let offset_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&offset_path)
                .expect("Failed to create/open offset file");
            offset_file
                .set_len(DiskStorageCore::FILE_SIZE)
                .expect("Failed to set file length of offset");
            let offset_mmap = unsafe {
                MmapMut::map_mut(&offset_file).expect("Failed to create memory map for offset")
            };
            let offset = FileMmap::new(offset_file, offset_mmap);

            let raft_metadata_path = PathBuf::from(format!("{}/raft_metadata", dir));
            let raft_metadata_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&raft_metadata_path)
                .expect("Failed to create/open raft_metadata file");
            raft_metadata_file
                .set_len((2 * size_of::<u64>() + 2) as u64)
                .expect("Failed to set file length of raft_file"); // we need 2 u64: first and last index and 2 bytes to check if they are set
            let raft_metadata = unsafe {
                MmapMut::map_mut(&raft_metadata_file)
                    .expect("Failed to create memory map for raft_metadata")
            };

            let conf_state = ConfState::new();
            let snapshot_metadata: SnapshotMetadata = Default::default();

            DiskStorageCore {
                dir: String::from(dir),
                conf_state,
                hard_state,
                log,
                offset,
                raft_metadata,
                num_entries: 0,
                snapshot_metadata,
                pending_conf_state: None,
                pending_conf_state_start_index: None,
            }
        }

        fn new_with_conf_state<T>(dir: &str, conf_state: T) -> DiskStorageCore
        where
            ConfState: From<T>,
        {
            let mut store = DiskStorageCore::new(dir);
            store.conf_state = ConfState::from(conf_state);
            store.snapshot_metadata.index = 1;
            store.snapshot_metadata.term = 1;
            (&mut store.hard_state[DiskStorageCore::TERM_INDEX])
                .write(&1u64.to_be_bytes())
                .expect("Failed to write hard state term");
            (&mut store.hard_state[DiskStorageCore::COMMIT_INDEX])
                .write(&1u64.to_be_bytes())
                .expect("Failed to write hard state commit");
            store
        }

        fn read_hard_state_field(&self, field: Range<usize>) -> u64 {
            let bytes = self
                .hard_state
                .get(field)
                .expect("Failed to read bytes from hard_state");
            u64::from_be_bytes(bytes.try_into().expect("Failed to deserialise to u64"))
        }

        fn get_hard_state(&self) -> HardState {
            let term = self.read_hard_state_field(DiskStorageCore::TERM_INDEX);
            let commit = self.read_hard_state_field(DiskStorageCore::COMMIT_INDEX);
            let vote = self.read_hard_state_field(DiskStorageCore::VOTE_INDEX);
            let mut hs = HardState::new();
            hs.set_term(term);
            hs.set_commit(commit);
            hs.set_vote(vote);
            hs
        }

        fn append_entries(&mut self, entries: &[Entry], from_log_index: u64) -> Result<(), Error> {
            // appends all entries starting from a given log index
            let from = if from_log_index >= self.num_entries {
                SeekFrom::Current(0)
            } else {
                SeekFrom::Start(self.get_log_offset(from_log_index) as u64)
            };
            self.log.file.seek(from)?;
            let num_added_entries = entries.len() as u64;
            let new_first_index = entries[0].index;
            let new_last_index = entries.last().unwrap().index;
            for (i, e) in entries.iter().enumerate() {
                let current_offset = self
                    .log
                    .file
                    .seek(SeekFrom::Current(0))
                    .expect("Failed to get current seeked offset"); // byte offset in log file
                let ser_entry = e
                    .write_to_bytes()
                    .expect("Protobuf failed to serialise Entry");
                let ser_entry_len = ser_entry.len() as u64;
                let ser_len = ser_entry_len.to_be_bytes();
                self.log.file.write(&ser_len)?; // write len of serialised entry
                match self.log.file.write(&ser_entry) {
                    // write entry
                    Ok(_) => {
                        // write to offset file
                        let start = (from_log_index as usize + i) * size_of::<u64>();
                        let stop = start + size_of::<u64>();
                        (&mut self.offset.mem_map[start..stop])
                            .write(&current_offset.to_be_bytes())
                            .expect("Failed to write to offset");
                    }
                    _ => panic!("Failed to write to log"),
                }
            }
            if from_log_index == 0 {
                self.set_raft_metadata(DiskStorageCore::FIRST_INDEX, new_first_index)
                    .expect("Failed to set first index metadata");
            }
            let num_removed_entries = self.num_entries - from_log_index;
            self.num_entries = self.num_entries + num_added_entries - num_removed_entries;
            self.set_raft_metadata(DiskStorageCore::LAST_INDEX, new_last_index)
                .expect("Failed to set last index metadata");
            self.log.file.flush()?;
            self.offset.file.flush()?;
            //        println!("N: {}, last_index: {}", self.num_entries, new_last_index);
            Ok(())
        }

        // returns byte offset in log from log_index
        fn get_log_offset(&self, log_index: u64) -> usize {
            let s = size_of::<u64>();
            let start = (log_index as usize) * s;
            let stop = start + s;
            let bytes = self.offset.mem_map.get(start..stop);
            let offset = u64::from_be_bytes(bytes.unwrap().try_into().unwrap());
            offset as usize
        }

        fn get_entry(&self, index: u64) -> Entry {
            let start = self.get_log_offset(index);
            let stop = start + size_of::<u64>();
            let entry_len = self.log.mem_map.get(start..stop);
            let des_entry_len = u64::from_be_bytes(entry_len.unwrap().try_into().unwrap());
            let r = stop..(stop + des_entry_len as usize);
            let entry = self.log.mem_map.get(r).unwrap_or_else(|| {
                panic!(
                    "Failed to get serialised entry in range {}..{}",
                    stop,
                    stop + des_entry_len as usize
                )
            });
            parse_from_bytes::<Entry>(entry).expect("Protobuf failed to deserialise entry")
        }

        fn set_raft_metadata(&mut self, field: Range<usize>, value: u64) -> Result<(), Error> {
            assert!(field == DiskStorageCore::FIRST_INDEX || field == DiskStorageCore::LAST_INDEX);
            match field {
                DiskStorageCore::FIRST_INDEX => {
                    (&mut self.raft_metadata[DiskStorageCore::FIRST_INDEX_IS_SET])
                        .write(&[1u8])
                        .expect("Failed to set first_index bit");
                }
                DiskStorageCore::LAST_INDEX => {
                    (&mut self.raft_metadata[DiskStorageCore::LAST_INDEX_IS_SET])
                        .write(&[1u8])
                        .expect("Failed to set last_index bit");
                }
                _ => panic!("Unexpected field"),
            }
            (&mut self.raft_metadata[field])
                .write(&value.to_be_bytes())
                .expect("Failed to write raft metadata");
            self.raft_metadata.flush()?;
            Ok(())
        }

        fn get_raft_metadata(&self, field: Range<usize>) -> Option<u64> {
            match field {
                DiskStorageCore::FIRST_INDEX => {
                    if self
                        .raft_metadata
                        .get(DiskStorageCore::FIRST_INDEX_IS_SET)
                        .unwrap()[0]
                        == 0
                    {
                        None
                    } else {
                        let bytes = self.raft_metadata.get(field).unwrap();
                        Some(u64::from_be_bytes(
                            bytes.try_into().expect("Could not deserialise to u64"),
                        ))
                    }
                }
                DiskStorageCore::LAST_INDEX => {
                    if self
                        .raft_metadata
                        .get(DiskStorageCore::LAST_INDEX_IS_SET)
                        .unwrap()[0]
                        == 0
                    {
                        None
                    } else {
                        let bytes = self.raft_metadata.get(field).unwrap();
                        Some(u64::from_be_bytes(
                            bytes.try_into().expect("Could not deserialise to u64"),
                        ))
                    }
                }
                _ => panic!("Got unexpected field in get_raft_metadata"),
            }
        }

        fn append_log(&mut self, entries: &[Entry]) -> Result<(), Error> {
            if entries.is_empty() {
                return Ok(());
            }
            let first_index = self.first_index().expect("Failed to get first index");
            if first_index > entries[0].index {
                panic!(
                    "overwrite compacted raft logs, compacted: {}, append: {}",
                    first_index - 1,
                    entries[0].index,
                );
            }
            let last_index = self.last_index().expect("Failed to get last index");
            if last_index + 1 < entries[0].index {
                panic!(
                    "raft logs should be continuous, last index: {}, new appended: {}",
                    last_index, entries[0].index,
                );
            }
            let diff = entries[0].index - first_index;
            self.append_entries(entries, diff)
        }

        fn set_conf_state(
            &mut self,
            cs: ConfState,
            pending_membership_change: Option<(ConfState, u64)>,
        ) {
            self.conf_state = cs;
            if let Some((cs, idx)) = pending_membership_change {
                self.pending_conf_state = Some(cs);
                self.pending_conf_state_start_index = Some(idx);
            }
        }

        fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
            (&mut self.hard_state[DiskStorageCore::TERM_INDEX]).write(&term.to_be_bytes())?;
            (&mut self.hard_state[DiskStorageCore::COMMIT_INDEX]).write(&commit.to_be_bytes())?;
            Ok(())
        }

        fn clear_dir(&mut self) -> Result<(), IOError> {
            match remove_dir_all(&self.dir) {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.kind() == NotFound {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }
    }

    impl Storage for DiskStorageCore {
        fn initial_state(&self) -> Result<RaftState, Error> {
            let hard_state = self.get_hard_state();
            let rs = RaftState::new(hard_state, self.conf_state.clone());
            Ok(rs)
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
        ) -> Result<Vec<Entry>, Error> {
            let first_index = self.first_index()?;
            if low < first_index {
                return Err(Error::Store(StorageError::Compacted));
            }
            let last_index = self.last_index()?;
            if high > last_index + 1 {
                panic!(
                    "index out of bound (last: {}, high: {})",
                    last_index + 1,
                    high
                );
            }
            let offset = first_index;
            let lo = low - offset;
            let hi = high - offset;
            let mut ents: Vec<Entry> = Vec::new();
            for i in lo..hi {
                ents.push(self.get_entry(i))
            }
            let max_size = max_size.into();
            limit_size(&mut ents, max_size);
            Ok(ents)
        }

        fn term(&self, idx: u64) -> Result<u64, Error> {
            if idx == self.snapshot_metadata.index {
                return Ok(self.snapshot_metadata.term);
            }
            let offset = self.first_index()?;
            if idx < offset {
                return Err(Error::Store(StorageError::Compacted));
            }
            let log_index = idx - offset;
            if log_index >= self.num_entries {
                println!(
                    "{}",
                    format!(
                        "log_index: {}, num_entries: {}, idx: {}, offset: {}",
                        log_index, self.num_entries, idx, offset
                    )
                );
                return Err(Error::Store(StorageError::Unavailable));
            }
            Ok(self.get_entry(log_index).term)
        }

        fn first_index(&self) -> Result<u64, Error> {
            match self.get_raft_metadata(DiskStorageCore::FIRST_INDEX) {
                Some(index) => Ok(index),
                None => Ok(self.snapshot_metadata.index + 1),
            }
        }

        fn last_index(&self) -> Result<u64, Error> {
            match self.get_raft_metadata(DiskStorageCore::LAST_INDEX) {
                Some(index) => Ok(index),
                None => Ok(self.snapshot_metadata.index),
            }
        }

        fn snapshot(&self, _request_index: u64) -> Result<Snapshot, Error> {
            unimplemented!();
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
        use std::fs::remove_dir_all;

        fn new_entry(index: u64, term: u64) -> Entry {
            let mut e = Entry::default();
            e.term = term;
            e.index = index;
            e
        }

        fn size_of<T: PbMessage>(m: &T) -> u32 {
            m.compute_size() as u32
        }

        #[test]
        fn diskstorage_term_test() {
            let ents = vec![new_entry(2, 2), new_entry(3, 3)];
            let mut tests = vec![
                (0, Err(Error::Store(StorageError::Compacted))),
                (2, Ok(2)),
                (3, Ok(3)),
                (6, Err(Error::Store(StorageError::Unavailable))),
            ];

            let mut store = DiskStorage::new_with_conf_state("term_test", (vec![1, 2, 3], vec![]));
            store.append_log(&ents).expect("Failed to append logs");

            for (i, (idx, wterm)) in tests.drain(..).enumerate() {
                let t = store.term(idx);
                if t != wterm {
                    panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
                }
            }
            remove_dir_all("term_test").expect("Failed to remove test storage files");
        }

        #[test]
        fn diskstorage_entries_test() {
            let ents = vec![
                new_entry(2, 2),
                new_entry(3, 3),
                new_entry(4, 4),
                new_entry(5, 5),
                new_entry(6, 6),
            ];
            let max_u64 = u64::max_value();
            let mut tests = vec![
                (0, 6, max_u64, Err(Error::Store(StorageError::Compacted))),
                (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
                (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
                (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                (
                    4,
                    7,
                    max_u64,
                    Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
                ),
                // even if maxsize is zero, the first entry should be returned
                (4, 7, 0, Ok(vec![new_entry(4, 4)])),
                // limit to 2
                (
                    4,
                    7,
                    u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                    Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
                ),
                (
                    4,
                    7,
                    u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                    Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
                ),
                (
                    4,
                    7,
                    u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                    Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
                ),
                // all
                (
                    4,
                    7,
                    u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                    Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
                ),
            ];
            let mut storage =
                DiskStorage::new_with_conf_state("entries_test", (vec![1, 2, 3], vec![]));
            storage.append_log(&ents).expect("Failed to append logs");
            for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
                let e = storage.entries(lo, hi, maxsize);
                if e != wentries {
                    panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
                }
            }
            remove_dir_all("entries_test").expect("Failed to remove test storage files");
        }

        #[test]
        fn diskstorage_overwrite_entries_test() {
            let ents = vec![
                new_entry(2, 2),
                new_entry(3, 3),
                new_entry(4, 3),
                new_entry(5, 3),
                new_entry(6, 3),
            ];
            let mut storage =
                DiskStorage::new_with_conf_state("overwrite_entries_test", (vec![1, 2, 3], vec![]));
            storage.append_log(&ents).expect("Failed to append logs");
            let overwrite_ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
            storage
                .append_log(&overwrite_ents)
                .expect("Failed to append logs");
            let tests = vec![
                (3, Ok(3)),
                (4, Ok(4)),
                (5, Ok(5)),
                (6, Err(Error::Store(StorageError::Unavailable))),
            ];
            for (idx, exp) in tests {
                let res = storage.term(idx as u64);
                assert_eq!(res, exp);
            }
            remove_dir_all("overwrite_entries_test").expect("Failed to remove test storage files");
        }

        #[test]
        fn diskstorage_metadata_test() {
            let ents = vec![
                new_entry(2, 2),
                new_entry(3, 3),
                new_entry(4, 4),
                new_entry(5, 5),
                new_entry(6, 6),
            ];
            let mut storage =
                DiskStorage::new_with_conf_state("metadata_test", (vec![1, 2, 3], vec![]));
            storage.append_log(&ents).expect("Failed to append logs");
            assert_eq!(storage.first_index(), Ok(2));
            assert_eq!(storage.last_index(), Ok(6));
            let overwrite_ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
            storage
                .append_log(&overwrite_ents)
                .expect("Failed to append logs");
            assert_eq!(storage.first_index(), Ok(2));
            assert_eq!(storage.last_index(), Ok(5));
            remove_dir_all("metadata_test").expect("Failed to remove test storage files");
        }
    }
}

pub mod paxos {
    use super::super::messages::paxos::ballot_leader_election::Ballot;
    use super::super::paxos::{raw_paxos::Entry, PaxosStateTraits, SequenceTraits};
    use crate::bench::atomic_broadcast::messages::paxos::PaxosSer;
    use std::fmt::Debug;
    use std::mem;
    use std::sync::Arc;

    pub trait Sequence {
        fn new() -> Self;

        fn new_with_sequence(seq: Vec<Entry>) -> Self;

        fn append_entry(&mut self, entry: Entry);

        fn append_sequence(&mut self, seq: &mut Vec<Entry>);

        fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry>);

        fn get_entries(&self, from: u64, to: u64) -> &[Entry];

        fn get_ser_entries(&self, from: u64, to: u64) -> Option<Vec<u8>>;

        fn get_suffix(&self, from: u64) -> Vec<Entry>;

        fn get_ser_suffix(&self, from: u64) -> Option<Vec<u8>>;

        fn get_sequence(&self) -> Vec<Entry>;

        fn get_sequence_len(&self) -> u64;

        fn stopped(&self) -> bool;
    }

    pub trait PaxosState {
        fn new() -> Self;

        fn set_promise(&mut self, nprom: Ballot);

        fn set_decided_len(&mut self, ld: u64);

        fn set_accepted_ballot(&mut self, na: Ballot);

        fn get_accepted_ballot(&self) -> Ballot;

        fn get_decided_len(&self) -> u64;

        fn get_promise(&self) -> Ballot;
    }

    enum PaxosSequence<S>
    where
        S: Sequence,
    {
        Active(S),
        Stopped(Arc<S>),
        None, // ugly intermediate value to use with mem::replace
    }

    pub struct Storage<S, P>
    where
        S: Sequence,
        P: PaxosState,
    {
        sequence: PaxosSequence<S>,
        paxos_state: P,
    }

    impl<S, P> Storage<S, P>
    where
        S: Sequence,
        P: PaxosState,
    {
        pub fn with(seq: S, paxos_state: P) -> Storage<S, P> {
            let sequence = PaxosSequence::Active(seq);
            Storage {
                sequence,
                paxos_state,
            }
        }

        pub fn append_entry(&mut self, entry: Entry) -> u64 {
            match &mut self.sequence {
                PaxosSequence::Active(s) => {
                    s.append_entry(entry);
                    s.get_sequence_len()
                }
                PaxosSequence::Stopped(_) => {
                    panic!("Sequence should not be modified after reconfiguration");
                }
                _ => panic!("Got unexpected intermediate PaxosSequence::None"),
            }
        }

        pub fn append_sequence(&mut self, sequence: &mut Vec<Entry>) -> u64 {
            match &mut self.sequence {
                PaxosSequence::Active(s) => {
                    s.append_sequence(sequence);
                    s.get_sequence_len()
                }
                PaxosSequence::Stopped(_) => {
                    panic!("Sequence should not be modified after reconfiguration");
                }
                _ => panic!("Got unexpected intermediate PaxosSequence::None"),
            }
        }

        pub fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry>) -> u64 {
            match &mut self.sequence {
                PaxosSequence::Active(s) => {
                    s.append_on_prefix(from_idx, seq);
                    s.get_sequence_len()
                }
                PaxosSequence::Stopped(s) => {
                    if &s.get_suffix(from_idx) != seq {
                        panic!("Sequence should not be modified after reconfiguration");
                    } else {
                        s.get_sequence_len()
                    }
                }
                _ => panic!("Got unexpected intermediate PaxosSequence::None"),
            }
        }

        pub fn append_on_decided_prefix(&mut self, seq: Vec<Entry>) {
            let from_idx = self.get_decided_len();
            match &mut self.sequence {
                PaxosSequence::Active(s) => {
                    let mut sequence = seq;
                    s.append_on_prefix(from_idx, &mut sequence);
                }
                PaxosSequence::Stopped(_) => {
                    if !seq.is_empty() {
                        panic!("Sequence should not be modified after reconfiguration");
                    }
                }
                _ => panic!("Got unexpected intermediate PaxosSequence::None"),
            }
        }

        pub fn set_promise(&mut self, nprom: Ballot) {
            self.paxos_state.set_promise(nprom);
        }

        pub fn set_decided_len(&mut self, ld: u64) {
            self.paxos_state.set_decided_len(ld);
        }

        pub fn set_accepted_ballot(&mut self, na: Ballot) {
            self.paxos_state.set_accepted_ballot(na);
        }

        pub fn get_accepted_ballot(&self) -> Ballot {
            self.paxos_state.get_accepted_ballot()
        }

        pub fn get_entries(&self, from: u64, to: u64) -> &[Entry] {
            match &self.sequence {
                PaxosSequence::Active(s) => s.get_entries(from, to),
                PaxosSequence::Stopped(s) => s.get_entries(from, to),
                _ => panic!("Got unexpected intermediate PaxosSequence::None in get_entries"),
            }
        }

        pub fn get_sequence_len(&self) -> u64 {
            match self.sequence {
                PaxosSequence::Active(ref s) => s.get_sequence_len(),
                PaxosSequence::Stopped(ref arc_s) => arc_s.get_sequence_len(),
                _ => panic!("Got unexpected intermediate PaxosSequence::None in get_sequence_len"),
            }
        }

        pub fn get_decided_len(&self) -> u64 {
            self.paxos_state.get_decided_len()
        }

        pub fn get_suffix(&self, from: u64) -> Vec<Entry> {
            match self.sequence {
                PaxosSequence::Active(ref s) => s.get_suffix(from),
                PaxosSequence::Stopped(ref arc_s) => arc_s.get_suffix(from),
                _ => panic!("Got unexpected intermediate PaxosSequence::None in get_suffix"),
            }
        }

        pub fn get_promise(&self) -> Ballot {
            self.paxos_state.get_promise()
        }

        pub fn stopped(&self) -> bool {
            match self.sequence {
                PaxosSequence::Active(ref s) => s.stopped(),
                PaxosSequence::Stopped(_) => true,
                _ => panic!("Got unexpected intermediate PaxosSequence::None in stopped()"),
            }
        }

        pub fn stop_and_get_sequence(&mut self) -> Arc<S> {
            let a = mem::replace(&mut self.sequence, PaxosSequence::None);
            match a {
                PaxosSequence::Active(s) => {
                    let arc_s = Arc::from(s);
                    self.sequence = PaxosSequence::Stopped(arc_s.clone());
                    arc_s
                }
                _ => panic!("Storage should already have been stopped!"),
            }
        }

        pub fn get_sequence(&self) -> Vec<Entry> {
            match &self.sequence {
                PaxosSequence::Active(s) => s.get_sequence(),
                PaxosSequence::Stopped(s) => s.get_sequence(),
                _ => panic!("Got unexpected intermediate PaxosSequence::None in get_sequence"),
            }
        }
    }

    #[derive(Debug)]
    pub struct MemorySequence {
        sequence: Vec<Entry>,
    }

    impl SequenceTraits for MemorySequence {}

    impl Sequence for MemorySequence {
        fn new() -> Self {
            MemorySequence { sequence: vec![] }
        }

        fn new_with_sequence(seq: Vec<Entry>) -> Self {
            MemorySequence { sequence: seq }
        }

        fn append_entry(&mut self, entry: Entry) {
            self.sequence.push(entry);
        }

        fn append_sequence(&mut self, seq: &mut Vec<Entry>) {
            self.sequence.append(seq);
        }

        fn append_on_prefix(&mut self, from_idx: u64, seq: &mut Vec<Entry>) {
            self.sequence.truncate(from_idx as usize);
            self.sequence.append(seq);
        }

        fn get_entries(&self, from: u64, to: u64) -> &[Entry] {
            match self.sequence.get(from as usize..to as usize) {
                Some(ents) => ents,
                None => panic!(
                    "get_entries out of bounds. From: {}, To: {}, len: {}",
                    from,
                    to,
                    self.sequence.len()
                ),
            }
        }

        fn get_ser_entries(&self, from: u64, to: u64) -> Option<Vec<u8>> {
            match self.sequence.get(from as usize..to as usize) {
                Some(ents) => {
                    let mut bytes = Vec::with_capacity(((to - from) * 8) as usize);
                    PaxosSer::serialise_entries(ents, &mut bytes);
                    Some(bytes)
                }
                _ => None,
            }
        }

        fn get_suffix(&self, from: u64) -> Vec<Entry> {
            match self.sequence.get(from as usize..) {
                Some(s) => s.to_vec(),
                None => vec![],
            }
        }

        fn get_ser_suffix(&self, from: u64) -> Option<Vec<u8>> {
            match self.sequence.get(from as usize..) {
                Some(s) => {
                    let len = s.len();
                    let mut bytes: Vec<u8> = Vec::with_capacity(len * 40);
                    PaxosSer::serialise_entries(s, &mut bytes);
                    Some(bytes)
                }
                None => None,
            }
        }

        fn get_sequence(&self) -> Vec<Entry> {
            self.sequence.clone()
        }

        fn get_sequence_len(&self) -> u64 {
            self.sequence.len() as u64
        }

        fn stopped(&self) -> bool {
            match self.sequence.last() {
                Some(entry) => entry.is_stopsign(),
                None => false,
            }
        }
    }

    #[derive(Debug)]
    pub struct MemoryState {
        n_prom: Ballot,
        acc_round: Ballot,
        ld: u64,
    }

    impl PaxosStateTraits for MemoryState {}

    impl PaxosState for MemoryState {
        fn new() -> Self {
            let ballot = Ballot::with(0, 0);
            MemoryState {
                n_prom: ballot,
                acc_round: ballot,
                ld: 0,
            }
        }

        fn set_promise(&mut self, nprom: Ballot) {
            self.n_prom = nprom;
        }

        fn set_decided_len(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn set_accepted_ballot(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_ballot(&self) -> Ballot {
            self.acc_round
        }

        fn get_decided_len(&self) -> u64 {
            self.ld
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }
    }
}
