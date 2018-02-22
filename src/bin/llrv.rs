extern crate clap;
#[macro_use]
extern crate lazy_static;
extern crate rand;

use clap::{App, Arg};
use rand::{thread_rng, Rng};
use std::net::UdpSocket;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time;
use std::thread;

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

fn main() {
    let matches = App::new("llrv")
        .about("stresses statsd servers")
        .arg(
            Arg::with_name("udp_port")
                .long("udp_port")
                .takes_value(true)
                .help("Sets the UDP port to ping")
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
            Arg::with_name("line_limit")
                .long("line_limit")
                .takes_value(true)
                .help("Maximum number of lines to emit in a statsd payload")
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

    let udp_port = matches
        .value_of("udp_port")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let pool_size = matches
        .value_of("pool_size")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let line_limit = matches
        .value_of("line_limit")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let delay_limit = matches
        .value_of("delay_limit")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0);
    let dest = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), udp_port);

    let socket = UdpSocket::bind(addr).unwrap();
    socket.set_nonblocking(true).unwrap();

    let mut pool: Vec<(String, &str)> = Vec::with_capacity(pool_size);
    let mut attempts = 10;
    let mut gauges = 0;
    let mut counters = 0;
    let mut histograms = 0;
    let mut timers = 0;
    while attempts > 0 {
        for _ in 0..pool_size {
            let metric_name: String = rng.gen_ascii_chars().take(6).collect();
            match pool.binary_search_by(|probe| probe.0.cmp(&metric_name)) {
                Ok(_) => {}
                Err(idx) => {
                    let metric_type: &str = match rng.gen_range(0, 100) {
                        98...100 => {
                            histograms += 1;
                            "h"
                        }
                        95...97 => {
                            timers += 1;
                            "ms"
                        }
                        45...94 => {
                            counters += 1;
                            "c"
                        }
                        _ => {
                            gauges += 1;
                            "g"
                        }
                    };
                    pool.insert(idx, (metric_name.clone(), metric_type));
                }
            };
        }
        if pool.len() == pool_size {
            break;
        }
        attempts -= 1;
    }

    let mut vals = Vec::with_capacity(1000);
    for i in 0..1000 {
        vals.push(i.to_string());
    }

    println!("POOL FILLED");
    println!("{:<2}GAUGES:     {}", "", gauges);
    println!("{:<2}COUNTERS:   {}", "", counters);
    println!("{:<2}HISTOGRAMS: {}", "", histograms);
    println!("{:<2}TIMERS:     {}", "", timers);

    let mut buf = String::new();
    loop {
        let choice = rng.choose(&pool).unwrap();
        let metric_name = &choice.0;
        let metric_type = &choice.1;
        let val = rng.choose(&vals).unwrap();

        let tot = rng.gen_range(1, 40);
        let lines_written = LINES_WRITTEN.fetch_add(tot, Ordering::Relaxed);
        for _ in 0..tot {
            buf.push_str("a");
            buf.push_str(metric_name);
            buf.push_str(":");
            buf.push_str(val);
            buf.push_str("|");
            buf.push_str(metric_type);
            buf.push_str("\n");
        }
        PACKETS_WRITTEN.fetch_add(1, Ordering::Relaxed);
        socket.send_to(buf.as_bytes(), dest).unwrap();
        buf.clear();
        if lines_written > line_limit {
            use std::time;
            let slp = time::Duration::from_millis(delay_limit as u64);
            thread::sleep(slp);
        }
    }
}
