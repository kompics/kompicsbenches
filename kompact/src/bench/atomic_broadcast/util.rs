pub mod exp_params {
    use std::time::Duration;
    pub const WINDOW_DURATION: Duration = Duration::from_millis(1000);
    pub const DATA_SIZE: usize = 8;
}

#[cfg(feature = "measure_io")]
pub mod io_metadata {
    use pretty_bytes::converter::convert;
    use std::{fmt, ops::Add};

    #[derive(Copy, Clone, Default, Eq, PartialEq)]
    pub struct IOMetaData {
        msgs_sent: usize,
        bytes_sent: usize,
        msgs_received: usize,
        bytes_received: usize,
    }

    impl IOMetaData {
        pub fn update_received<T>(&mut self, msg: &T) {
            let size = std::mem::size_of_val(msg);
            self.bytes_received += size;
            self.msgs_received += 1;
        }

        pub fn update_sent<T>(&mut self, msg: &T) {
            let size = std::mem::size_of_val(msg);
            self.bytes_sent += size;
            self.msgs_sent += 1;
        }

        pub fn update_sent_with_size(&mut self, size: usize) {
            self.bytes_sent += size;
            self.msgs_sent += 1;
        }

        pub fn update_received_with_size(&mut self, size: usize) {
            self.bytes_received += size;
            self.msgs_received += 1;
        }

        pub fn reset(&mut self) {
            self.msgs_received = 0;
            self.bytes_received = 0;
            self.msgs_sent = 0;
            self.bytes_sent = 0;
        }
    }

    impl fmt::Debug for IOMetaData {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_fmt(format_args!(
                "Sent: ({}, {:?}), Received: ({}, {:?})",
                self.msgs_sent,
                &convert(self.bytes_sent as f64),
                self.msgs_received,
                &convert(self.bytes_received as f64)
            ))
        }
    }

    impl Add for IOMetaData {
        type Output = Self;

        fn add(self, other: Self) -> Self {
            Self {
                msgs_received: self.msgs_received + other.msgs_received,
                bytes_received: self.bytes_received + other.bytes_received,
                msgs_sent: self.msgs_sent + other.msgs_sent,
                bytes_sent: self.bytes_sent + other.bytes_sent,
            }
        }
    }
}
