use std::{fs};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::fs::{DirEntry, File};
use std::io::{BufReader, Read};
use std::path::{PathBuf};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvError};
use rand::prelude::SliceRandom;

use crate::error::DataLoadError;

pub trait DataLoader<O,E> where E: Error + Debug + Display {
    fn load(&mut self) -> Result<Option<O>,E>;
}
pub struct DataLoaderBuilder {
    sfen_size:usize,
    batch_size:usize,
    read_sfen_size:usize,
    shuffle:bool,
    search_dir:PathBuf,
    ext:String,
    start_filename:Option<String>,
    processed_items:usize,
    resume:bool
}
impl DataLoaderBuilder {
    pub fn new(search_dir:PathBuf) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:38,
            batch_size:256,
            read_sfen_size: 102400,
            shuffle:false,
            search_dir:search_dir,
            ext:String::from("hcpe"),
            start_filename:None,
            processed_items:0,
            resume:false
        }
    }

    pub fn sfen_size(self,sfen_size:usize) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn batch_size(self,batch_size:usize) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn read_sfen_size(self,read_sfen_size:usize) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn shuffle(self,shuffle:bool) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn seach_dir(self,search_dir:PathBuf) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn ext(self,ext:String) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn start_filename(self, current_filename:Option<String>) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:current_filename,
            processed_items:self.processed_items,
            resume:self.resume
        }
    }

    pub fn processed_items(self, current_items:usize) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:current_items,
            resume:self.resume
        }
    }

    pub fn resume(self,resume:bool) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            start_filename:self.start_filename,
            processed_items:self.processed_items,
            resume:resume
        }
    }

    pub fn build<F,O,E>(self,processer:F) -> Result<UnifiedDataLoader<O,E>,DataLoadError>
        where O: Send + 'static,
              E: Error + Debug + Display + From<DataLoadError> + Send + 'static,
              F: FnMut(Vec<Vec<u8>>) -> Result<Option<O>,E> + Send + 'static {
        UnifiedDataLoader::new(
            processer,
            self.sfen_size,
            self.batch_size,
            self.read_sfen_size,
            self.ext,
            self.shuffle,
            self.search_dir,
            self.start_filename,
            self.processed_items,
            self.resume
        )
    }
}
pub struct UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + Display + From<DataLoadError> + Send + 'static {
    working:Arc<AtomicBool>,
    receiver: Receiver<Result<Option<(String,usize,O)>,E>>,
}
impl<O,E> UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + From<DataLoadError> + Send + 'static {
    fn new<F>(mut processer:F,
              sfen_size:usize,
              batch_size:usize,
              read_sfen_size:usize,
              ext:String,
              shuffle:bool,
              search_dir:PathBuf,
              start_filename:Option<String>,
              processed_items:usize,
              mut resume:bool) -> Result<UnifiedDataLoader<O,E>,DataLoadError>
    where F: FnMut(Vec<Vec<u8>>) -> Result<Option<O>,E> + Send + 'static {
        let (sender,r) = mpsc::channel();

        let working = Arc::new(AtomicBool::new(true));

        let mut current_filename = start_filename.clone().unwrap_or(String::from(""));

        {
            let working = Arc::clone(&working);

            let s = sender.clone();

            std::thread::Builder::new()
                .stack_size(1024 * 1024 * 1024)
                .spawn(move || {
                    if let Err(e) = Self::run(move || {
                        let mut rng = rand::thread_rng();

                        let mut skip_files = true;

                        let mut paths = fs::read_dir(&search_dir.as_path())?
                            .into_iter()
                            .collect::<Vec<Result<DirEntry,_>>>();
                        paths.sort_by(Self::cmp);

                        'outer: for path in paths {
                            if !working.load(Ordering::Acquire) {
                                break;
                            }

                            let mut current_items = 0;

                            let mut items = 0;

                            let path = path?.path();

                            let next_filename = path.as_path().file_name().map(|s| {
                                s.to_string_lossy().to_string()
                            });

                            if skip_files && next_filename.as_ref().map(|n| {
                                current_filename == *n
                            }).unwrap_or(false) {
                                skip_files = false;
                            }

                            current_filename = next_filename.unwrap_or(String::from(""));

                            if !path.as_path().extension().map(|e| e == ext.as_str()).unwrap_or(false) {
                                continue;
                            }

                            let metadata = fs::metadata(&path)?;

                            let mut remaining = metadata.len() as usize;

                            let mut reader = BufReader::new(File::open(path)?);

                            if resume {
                                reader.seek_relative((sfen_size * processed_items) as i64)?;

                                if remaining < sfen_size * current_items {
                                    return Err(DataLoadError::InvalidStateError(
                                        String::from(
                                            "The value of the number of trained items in the teacher phase is incorrect."
                                        )
                                    ));
                                }

                                remaining -= sfen_size * current_items;
                                current_items = processed_items;
                                items += processed_items;
                                resume = false;
                            }

                            while remaining > 0 {
                                if !working.load(Ordering::Acquire) {
                                    break 'outer;
                                }

                                let read_size = if remaining < sfen_size * read_sfen_size {
                                    remaining / sfen_size
                                } else {
                                    read_sfen_size
                                };

                                let mut buffer = vec![0; read_size * sfen_size];

                                reader.read_exact(&mut buffer)?;

                                current_items += read_size;

                                let mut buffer = buffer.chunks(sfen_size)
                                                       .into_iter().map(|p| p.to_vec())
                                                       .collect::<Vec<Vec<u8>>>();
                                if shuffle {
                                    buffer.shuffle(&mut rng);
                                }

                                let mut it = buffer.into_iter();

                                for _ in 0..((read_size + batch_size - 1) / batch_size) {
                                    if !working.load(Ordering::Acquire) {
                                        break 'outer;
                                    }

                                    let mut batch = Vec::with_capacity(batch_size);

                                    let mut j = 0;

                                    while let Some(p) = it.next() {
                                        j += 1;

                                        batch.push(p.to_vec());

                                        if j == batch_size {
                                            break;
                                        }
                                    }

                                    items += 1;

                                    let _ = s.send(processer(batch).map(|o| {
                                        o.map(|o| (current_filename.clone(),items,o))
                                    }));
                                }
                            }
                        }

                        let _ = s.send(Ok(None));

                        Ok(())
                    }) {
                        let _ = sender.send(Err(E::from(e)));
                    }
                })?;
        }

        Ok(UnifiedDataLoader {
            working:working,
            receiver:r
        })
    }

    fn run<F: FnMut() -> Result<(),DataLoadError>>(mut runner:F) -> Result<(),DataLoadError> {
        runner()
    }
    fn cmp(a:&Result<DirEntry,std::io::Error>,b:&Result<DirEntry,std::io::Error>) -> core::cmp::Ordering {
        match (a,b) {
            (Ok(a),Ok(b)) => {
                let a = a.file_name();
                let b = b.file_name();
                a.cmp(&b)
            },
            _ => {
                std::cmp::Ordering::Equal
            }
        }
    }
}
impl<O,E> DataLoader<(String,usize,O),E> for UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + Display + From<RecvError> + From<DataLoadError> + Send + 'static {
    fn load(&mut self) -> Result<Option<(String,usize,O)>,E> {
        Ok(self.receiver.recv()??)
    }
}
impl<O,E> Drop for UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + Display + From<DataLoadError> + Send + 'static {
    fn drop(&mut self) {
        self.working.store(false,Ordering::Release);
    }
}