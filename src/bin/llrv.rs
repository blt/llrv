extern crate rand;

#[macro_use]
extern crate lazy_static;

use rand::{thread_rng, Rng};
use std::net::UdpSocket;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::env;
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
    let _join = thread::spawn(move || tick());

    let mut rng = thread_rng();

    let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0);
    let dest = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 8125);

    let socket = UdpSocket::bind(addr).unwrap();
    socket.set_nonblocking(true).unwrap();

    // let types = ["c", "ms", "h", "g"];
    let types = ["ms"];
    // let types = ["c", "g"];

    let pool_size = match env::args().nth(1) {
        None => 10_000,
        Some(i_str) => i_str.parse::<usize>().unwrap(),
    };
    let line_limit = match env::args().nth(2) {
        None => 10_000,
        Some(i_str) => i_str.parse::<usize>().unwrap(),
    };

    let mut pool: Vec<(String, &str)> = Vec::with_capacity(pool_size);
    let mut attempts = 10;
    while attempts > 0 {
        for _ in 0..pool_size {
            let metric_name: String = rng.gen_ascii_chars().take(6).collect();
            match pool.binary_search_by(|probe| probe.1.cmp(&metric_name)) {
                Ok(_) => {}
                Err(idx) => {
                    let metric_type: &str = rng.choose(&types).unwrap();
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
            let slp = time::Duration::from_millis(100);
            thread::sleep(slp);
        }
    }
}
