extern crate clap;
extern crate rand;

use clap::{App, Arg};
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::convert::AsRef;
use std::path::{Path, PathBuf};
use std::io::Write;
use std::{fs, io, thread, time};

static LINES_WRITTEN: AtomicUsize = AtomicUsize::new(0);

enum Actions {
    Delete,
    Emit,
    Rotate,
    Truncate,
}

fn report() -> () {
    let one_second = time::Duration::from_millis(1_000);
    loop {
        thread::sleep(one_second);
        println!(
            "LINES WRITTEN: {}",
            LINES_WRITTEN.swap(0, Ordering::Relaxed)
        );
    }
}

fn assure_directory<P>(dir: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    fs::DirBuilder::new().recursive(true).create(dir)
}

fn tick(root_dir: PathBuf, files_per_thread: u8, max_line_length: u16) -> () {
    let mut rng = thread_rng();
    let mut paths = Vec::new();
    let mut fps: Vec<Option<io::BufWriter<fs::File>>> = Vec::new();

    let mut pool = Vec::with_capacity(1024);
    for _ in 0..1024 {
        let sz: u16 = rng.gen_range(16, 4096);
        let line: String = rng.gen_ascii_chars()
            .take((sz as usize) % (max_line_length as usize))
            .collect();
        pool.push(line);
    }

    for _ in 0..files_per_thread {
        let mut path = PathBuf::new().join(root_dir.clone());
        assure_directory(&path).expect("could not create directory");
        let log_name: String = rng.gen_ascii_chars().take(32).collect();
        path = path.join(log_name);
        path.set_extension("log");
        paths.push(path);
        fps.push(None);
    }

    assert_eq!(paths.len(), fps.len());

    loop {
        let action = match rng.gen_range(0, 1_000_000) {
            0 => Actions::Delete,
            1 => Actions::Truncate,
            2 => Actions::Rotate,
            _ => Actions::Emit,
        };

        let idx = rng.gen_range(0, files_per_thread as usize);
        if fps[idx].is_none() {
            let fp = io::BufWriter::new(
                fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(&paths[idx])
                    .expect(&format!("could not open: {:?}", paths[idx])),
            );
            fps[idx] = Some(fp);
        }
        let path = &paths[idx];
        match action {
            Actions::Delete => {
                if let Ok(()) = fs::remove_file(path) {
                    fps[idx] = None;
                }
            }
            Actions::Truncate => {
                fps[idx] = None;
                let fp = io::BufWriter::new(
                    fs::OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .truncate(true)
                        .open(path)
                        .unwrap(),
                );
                fps[idx] = Some(fp);
            }
            Actions::Emit => {
                let line = rng.choose(&pool[..]).unwrap();
                if let Some(ref mut fp) = fps[idx] {
                    fp.write(line.as_bytes()).expect("could not write");
                    fp.flush().expect("could not flush");
                } else {
                    unreachable!()
                }
                LINES_WRITTEN.fetch_add(1, Ordering::Relaxed);
            }
            Actions::Rotate => {
                fps[idx] = None;
                match fs::rename(path, format!("{}.1", path.to_str().unwrap())) {
                    Ok(()) => {
                        let fp = io::BufWriter::new(
                            fs::OpenOptions::new()
                                .create(true)
                                .read(true)
                                .write(true)
                                .truncate(true)
                                .open(path)
                                .expect(&format!("could not open: {:?}", paths[idx])),
                        );
                        fps[idx] = Some(fp);
                    }
                    Err(_) => {
                        let fp = io::BufWriter::new(
                            fs::OpenOptions::new()
                                .create(true)
                                .read(true)
                                .append(true)
                                .write(true)
                                .truncate(true)
                                .open(path)
                                .expect(&format!("could not open: {:?}", paths[idx])),
                        );
                        fps[idx] = Some(fp);
                    }
                }
            }
        }
    }
}

fn main() {
    let matches = App::new("log_gen")
        .about("stress log tailers")
        .arg(
            Arg::with_name("root_dir")
                .long("root_dir")
                .takes_value(true)
                .help("Directory to write into")
                .required(true),
        )
        .arg(
            Arg::with_name("files_per_thread")
                .long("files_per_thread")
                .takes_value(true)
                .help("Total number of files per thread")
                .required(true),
        )
        .arg(
            Arg::with_name("max_threads")
                .long("max_threads")
                .takes_value(true)
                .help("Total number of concurrent threads to write from")
                .required(true),
        )
        .arg(
            Arg::with_name("max_line_length")
                .long("max_line_length")
                .takes_value(true)
                .help("Maximum length of a log line")
                .required(true),
        )
        .get_matches();

    let mut threads = Vec::new();

    let root_dir = matches.value_of("root_dir").unwrap();
    let files_per_thread = matches
        .value_of("files_per_thread")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    let max_threads = matches
        .value_of("max_threads")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    let max_line_length = matches
        .value_of("max_line_length")
        .unwrap()
        .parse::<u16>()
        .unwrap();

    assure_directory(root_dir).expect("Could not create root_dir");

    let root_dir: PathBuf = PathBuf::new().join(root_dir);
    for _ in 0..max_threads {
        let thr_root_dir = root_dir.clone();
        threads.push(thread::spawn(move || {
            tick(thr_root_dir, files_per_thread, max_line_length)
        }));
    }

    thread::spawn(report);

    for handle in threads {
        handle.join().unwrap();
    }
}
