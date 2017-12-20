#[macro_use]
extern crate lazy_static;
extern crate llrv;
extern crate rand;
extern crate protobuf;
extern crate byteorder;

use std::net::{TcpStream, TcpListener};
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use std::io::Read;
use llrv::protocols::native::Payload;
use rand::{thread_rng, Rng};
use std::thread;
use std::collections::{VecDeque, HashMap};
use std::path;
use std::io;
use std::fs::OpenOptions;
use std::sync::Mutex;
use std::fs;
use std::io::Write;

lazy_static! {
    static ref LINESET: Mutex<HashMap<path::PathBuf, VecDeque<String>>> = Mutex::new(HashMap::default());
}

fn handle_client(stream: TcpStream) {
    let mut buf = Vec::with_capacity(4000);
    let mut reader = io::BufReader::new(stream);

    let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
        Ok(i) => i as usize,
        Err(_) => return,
    };
    println!("PAYLOAD SIZE: {:?}", payload_size_in_bytes);
    buf.resize(payload_size_in_bytes, 0);
    if reader.read_exact(&mut buf).is_err() {
        return;
    }
    match protobuf::parse_from_bytes::<Payload>(&buf) {
        Ok(mut pyld) => {
            for mut line in pyld.take_lines().into_iter() {
                let path: path::PathBuf = path::PathBuf::from(line.take_path());
                let value: String = line.take_value();

                println!("PATH: {:?} | VALUE: {:?}", path, value);
                
                let mut lineset = LINESET.lock().unwrap();
                let ent: &mut VecDeque<String> = lineset.get_mut(&path).unwrap();

                let expected: String = ent.pop_front().unwrap();
                assert_eq!(expected, value);
            }
        }
        Err(_) => {
            return;
        }
    }
}

fn recv() {
    println!("HELLO THIS IS DOG");
    let listener = TcpListener::bind("127.0.0.1:1972").unwrap();

    for stream in listener.incoming() {
        println!("NEW STREAM");
        thread::spawn(|| handle_client(stream.unwrap()));
    }
    println!("BYE NOW THANK YOU");
}

enum Action {
    Delete,
    Create,
    WriteTo,
    Rotate,
    Truncate,
}

fn gen() {
    let root = "/tmp/log_gen";

    let mut rng = thread_rng();
    let mut pool: Vec<String> = Vec::new();
    for _ in 0 .. 16 {
        for sz in &[3,4,5] {
            let s: String = rng.gen_ascii_chars().take(*sz).collect();        
            match pool.binary_search(&s) {
                Ok(_) => continue,
                Err(i) => {
                    pool.insert(i, s);
                }
            }
        }
    }
    
    let mut paths: Vec<path::PathBuf> = Vec::new();
    let mut fps = HashMap::new();


    loop {
        let action = match rng.gen_range(0, 100) {
            0...4 => Action::Delete,
            5...15 => Action::Create,
            16...25 => Action::Rotate,
            26...30 => Action::Truncate,
            _ => Action::WriteTo,
        };

        match action {
            Action::Delete => {
                if paths.is_empty() {
                    continue;
                }

                let idx = rng.gen_range(0, paths.len());
                fps.remove(&paths[idx]);
                std::fs::remove_file(&paths[idx]).unwrap();
                paths.remove(idx);
            }
            Action::Create => {
                let mut path = path::PathBuf::new();
                path.push(root);
                for _ in 0..rng.gen_range(0, 2) {
                    let dir = rng.choose(&pool).unwrap(); 
                    path.push(dir);                    
                }
                fs::create_dir_all(&path).unwrap();
                let log_name = rng.choose(&pool).unwrap();
                path.push(log_name);
                path.set_extension("log");
                
                fps.entry(path.clone()).or_insert_with(|| {
                    let fp = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(path.clone()).unwrap();
                    paths.push(path);
                    fp
                });
            },
            Action::WriteTo => {
                if let Some(ref path) = rng.choose(&paths) {
                    let mut fp = fps.get(*path).unwrap();
                    
                    let len = rng.gen_range(1, 2048);
                    let s: String = rng.gen_ascii_chars().take(len).collect();
                    assert!(fp.write(s.as_bytes()).is_ok());
                    let mut lineset = LINESET.lock().unwrap();
                    let ent: &mut VecDeque<String> = lineset.entry((*path).clone()).or_insert(VecDeque::new());
                    ent.push_back(s.clone());
                    assert!(fp.write("\n".as_bytes()).is_ok());
                    assert!(fp.flush().is_ok());
                }
            },
            Action::Rotate => {
                if let Some(ref path) = rng.choose(&paths) {
                let mut new_path = (*path).clone();
                new_path.set_extension("log.1");
                
                match fs::rename(&path, &new_path) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("ERROR: {:?}", e);
                        assert!(false);
                    }
                }
                let fp = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path).unwrap();
                    fps.insert((*path).clone().to_path_buf(), fp);
                }
            },
            Action::Truncate => {
                if let Some(ref path) = rng.choose(&paths) {
                    let fp = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(path).unwrap();
                    
                    fps.insert((*path).clone().to_path_buf(), fp);
                }
            }
        }
    }
}

fn main() {

    let gen_join = thread::spawn(gen);
    let recv_join = thread::spawn(recv);

    gen_join.join().unwrap();
    recv_join.join().unwrap();
}
