use std::error::Error;
use std::{error, fmt, io};
use std::sync::{PoisonError};
use std::sync::mpsc::RecvError;

/// Error during loading of teacher phase data
#[derive(Debug)]
pub enum DataLoadError {
    IOError(io::Error),
    MutexLockFailedError(String),
    RecvError(RecvError),
    InvalidStateError(String)
}
impl fmt::Display for DataLoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            &DataLoadError::IOError(err) => write!(f,"An IO error occurred while loading data for the teacher phase. ({})",err),
            &DataLoadError::MutexLockFailedError(err) => write!(f,"{}",err),
            &DataLoadError::RecvError(err) => write!(f,"{}",err),
            &DataLoadError::InvalidStateError(err) => write!(f,"{}",err),
        }
    }
}
impl Error for DataLoadError {
    fn description(&self) -> &str {
        match &self {
            &DataLoadError::IOError(_) => "An IO error occurred while loading data for the teacher phase.",
            &DataLoadError::MutexLockFailedError(_) => "An error occurred when locking the mutex.",
            &DataLoadError::RecvError(_) => "An error occurred while receiving the message.",
            &DataLoadError::InvalidStateError(_) => "Invalid state.",
        }
    }

    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self {
            &DataLoadError::IOError(ref err) => Some(err),
            &DataLoadError::MutexLockFailedError(_) => None,
            &DataLoadError::RecvError(ref err) => Some(err),
            &DataLoadError::InvalidStateError(_) => None,
        }
    }
}

impl From<io::Error> for DataLoadError {
    fn from(err: io::Error) -> DataLoadError {
        DataLoadError::IOError(err)
    }
}
impl<T> From<PoisonError<T>> for DataLoadError {
    fn from(err: PoisonError<T>) -> Self {
        DataLoadError::MutexLockFailedError(format!("{}",err))
    }
}
impl From<RecvError> for DataLoadError {
    fn from(err: RecvError) -> DataLoadError {
        DataLoadError::RecvError(err)
    }
}
