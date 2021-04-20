#[cfg(feature = "measure_io")]
use std::time::Duration;
#[cfg(feature = "measure_io")]
pub const LOG_IO_PERIOD: Duration = Duration::from_millis(5000);
pub const DATA_SIZE: usize = 8;
