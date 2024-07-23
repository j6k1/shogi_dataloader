use std::{fs};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::fs::{DirEntry, File};
use std::io::{BufReader, Read};
use std::path::{PathBuf};
use std::sync::{Arc, LockResult, mpsc, RwLock, RwLockReadGuard};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    current_filename:Option<String>,
    current_items:usize,
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
            current_filename:None,
            current_items:0,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
            resume:self.resume
        }
    }

    pub fn current_filename(self,current_filename:Option<String>) -> DataLoaderBuilder {
        DataLoaderBuilder {
            sfen_size:self.sfen_size,
            batch_size:self.batch_size,
            read_sfen_size:self.read_sfen_size,
            shuffle:self.shuffle,
            search_dir:self.search_dir,
            ext:self.ext,
            current_filename:current_filename,
            current_items:self.current_items,
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
            current_filename:self.current_filename,
            current_items:self.current_items,
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
            self.current_filename,
            self.current_items,
            self.resume
        )
    }
}
pub struct UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + Display + From<DataLoadError> + Send + 'static {
    working:Arc<AtomicBool>,
    receiver: Receiver<Result<Option<O>,E>>,
    current_filename:Arc<RwLock<Option<String>>>,
    current_items:Arc<AtomicUsize>
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
              current_filename:Option<String>,
              current_items:usize,
              resume:bool) -> Result<UnifiedDataLoader<O,E>,DataLoadError>
    where F: FnMut(Vec<Vec<u8>>) -> Result<Option<O>,E> + Send + 'static {
        let (sender,r) = mpsc::channel();

        let working = Arc::new(AtomicBool::new(true));

        let current_filename = Arc::new(RwLock::new(current_filename));
        let current_items = Arc::new(AtomicUsize::new(current_items));

        {
            let current_filename = Arc::clone(&current_filename);
            let current_items = Arc::clone(&current_items);

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

                            let path = path?.path();

                            print!("{}\n", path.display());

                            let next_filename = path.as_path().file_name().map(|s| {
                                s.to_string_lossy().to_string()
                            });

                            current_filename.write().map(|mut f| {
                                if skip_files && f.as_ref().and_then(|f| next_filename.as_ref().map(move |n| f == n)).unwrap_or(true) {
                                    skip_files = false;

                                    f.as_ref().map(|f| {
                                        println!("Processing starts from {}th item of file {}", current_items.load(Ordering::Acquire), f);
                                    });
                                }

                                *f = Some(next_filename.unwrap_or(String::from("")));
                            })?;

                            if !path.as_path().extension().map(|e| e == ext.as_str()).unwrap_or(false) {
                                continue;
                            }

                            let metadata = fs::metadata(&path)?;

                            let mut remaining = metadata.len() as usize;

                            let mut reader = BufReader::new(File::open(path)?);

                            if resume {
                                reader.seek_relative((sfen_size * current_items.load(Ordering::Acquire)) as i64)?;

                                if remaining < sfen_size * current_items.load(Ordering::Acquire) {
                                    return Err(DataLoadError::InvalidStateError(
                                        String::from(
                                            "The value of the number of trained items in the teacher phase is incorrect."
                                        )
                                    ));
                                }

                                remaining -= sfen_size * current_items.load(Ordering::Acquire);
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

                                current_items.fetch_add(read_size,Ordering::Release);

                                if shuffle {
                                    buffer.shuffle(&mut rng);
                                }

                                let mut it = buffer.chunks(sfen_size).into_iter();

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

                                    let _ = s.send(processer(batch));
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
            receiver:r,
            current_filename:current_filename,
            current_items:current_items
        })
    }

    fn run<F: FnMut() -> Result<(),DataLoadError>>(mut runner:F) -> Result<(),DataLoadError> {
        runner()
    }

    pub fn current_filename(&self) -> LockResult<RwLockReadGuard<'_, Option<String>>> {
        self.current_filename.read()
    }

    pub fn current_items(&self) -> usize {
        self.current_items.load(Ordering::Acquire)
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
impl<O,E> DataLoader<O,E> for UnifiedDataLoader<O,E>
    where O: Send + 'static,
          E: Error + Debug + Display + From<RecvError> + From<DataLoadError> + Send + 'static {
    fn load(&mut self) -> Result<Option<O>,E> {
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