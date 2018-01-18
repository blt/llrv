extern crate byteorder;
extern crate clap;
#[macro_use]
extern crate lazy_static;
extern crate llrv;
extern crate protobuf;
extern crate rand;

use clap::{App, Arg};
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time;
use std::io::BufWriter;
use byteorder::{BigEndian, ByteOrder};
use std::thread;
use llrv::protocols::native::*;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protobuf::stream::CodedOutputStream;

lazy_static! {
    static ref LINES_WRITTEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    static ref PACKETS_WRITTEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

fn tick() {
    loop {
        let packets = PACKETS_WRITTEN.swap(0, Ordering::Relaxed);
        let lines = LINES_WRITTEN.swap(0, Ordering::Relaxed);
        println!(
            "LINES PER SECOND: {} | TOTAL PACKETS PER SECOND: {}",
            lines, packets
        );
        let second = time::Duration::from_millis(1000);
        thread::sleep(second);
    }
}

fn connect(host: &str, port: u16) -> Option<TcpStream> {
    if let Ok(srv) = (host, port).to_socket_addrs() {
        let ips: Vec<_> = srv.collect();
        for ip in ips {
            if let Ok(stream) = TcpStream::connect(ip) {
                return Some(stream);
            }
        }
    }
    None
}

fn main() {
    let matches = App::new("llrv")
        .about("stresses cernan native servers")
        .arg(
            Arg::with_name("port")
                .long("port")
                .takes_value(true)
                .help("Sets the port to hit")
                .required(true),
        )
        .arg(
            Arg::with_name("host")
                .long("host")
                .takes_value(true)
                .help("Sets the host to hit")
                .required(true),
        )
        .arg(
            Arg::with_name("pool_size")
                .long("pool_size")
                .takes_value(true)
                .help("Total size of potential metric names to emit")
                .required(true),
        )
        .arg(
            Arg::with_name("payload_limit")
                .long("payload_limit")
                .takes_value(true)
                .help("Maximum number of points to emit in a payload")
                .required(true),
        )
        .arg(
            Arg::with_name("delay_limit")
                .long("delay_limit")
                .takes_value(true)
                .help("Total number of milliseconds to wait between emitting payloads")
                .required(true),
        )
        .get_matches();

    let _join = thread::spawn(move || tick());

    let mut rng = thread_rng();

    let host = matches.value_of("host").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>().unwrap();
    let pool_size = matches
        .value_of("pool_size")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let payload_limit = matches
        .value_of("payload_limit")
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let delay_limit = matches
        .value_of("delay_limit")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let types = [
        AggregationMethod::BIN,
        AggregationMethod::SET,
        AggregationMethod::SUM,
        AggregationMethod::SUMMARIZE,
    ];

    let mut pool: Vec<(String, AggregationMethod, bool)> = Vec::with_capacity(pool_size);
    let mut attempts = 10;
    while attempts > 0 {
        for _ in 0..pool_size {
            let metric_name: String = rng.gen_ascii_chars().take(6).collect();
            match pool.binary_search_by(|probe| probe.0.cmp(&metric_name)) {
                Ok(_) => {}
                Err(idx) => {
                    let metric_type: &AggregationMethod = rng.choose(&types).unwrap();
                    let persist: bool = rng.gen::<bool>();
                    pool.insert(idx, (metric_name.clone(), *metric_type, persist));
                }
            };
        }
        if pool.len() == pool_size {
            break;
        }
        attempts -= 1;
    }

    println!("POOL FILLED");

    let mut stream = None;
    loop {
        let mut points = Vec::new();
        loop {
            let choice = rng.choose(&pool).unwrap();
            let metric_name = &choice.0;
            let metric_type = &choice.1;
            let metric_persist = &choice.2;

            let mut point = Telemetry::new();
            point.set_name(metric_name.to_string());
            point.set_persisted(*metric_persist);
            point.set_method(*metric_type);
            let mut vals = Vec::new();
            for _ in 0..rng.gen_range(0, 50) {
                vals.push(rng.gen::<f64>());
            }
            point.set_samples(vals);

            points.push(point);

            if rng.gen_weighted_bool(payload_limit) {
                break;
            }
        }
        LINES_WRITTEN.fetch_add(points.len(), Ordering::Relaxed);

        let mut pyld = Payload::new();
        pyld.set_points(RepeatedField::from_vec(points));

        let mut delivery_failure = false;
        if let Some(ref mut strm) = stream {
            let mut bufwrite = BufWriter::new(strm);
            let mut strm = CodedOutputStream::new(&mut bufwrite);
            let mut sz_buf = [0; 4];
            let pyld_len = pyld.compute_size();
            BigEndian::write_u32(&mut sz_buf, pyld_len);
            strm.write_raw_bytes(&sz_buf).unwrap();
            let res = pyld.write_to_with_cached_sizes(&mut strm);
            if res.is_err() {
                delivery_failure = true;
            } else {
                PACKETS_WRITTEN.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            use std::time;
            let slp = time::Duration::from_millis(delay_limit as u64);
            thread::sleep(slp);
            stream = connect(&host, port);
        }
        if delivery_failure || rng.gen_weighted_bool(128) {
            stream = None
        }
    }
}
