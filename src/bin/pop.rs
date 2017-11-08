extern crate faktory;

use std::io;
use std::env;
use std::sync::atomic;

fn main() {
    let num = env::args()
        .skip(1)
        .next()
        .and_then(|v| i64::from_str_radix(&*v, 10).ok());
    if num.is_none() {
        eprintln!("usage: push N");
        return;
    }
    let mut num = num.unwrap();

    use std::thread;
    use std::time;
    thread::sleep(time::Duration::from_millis(300));
    let mut c = faktory::ConsumerBuilder::default();
    let i = atomic::AtomicUsize::new(0);
    c.register("bench", move |_| {
        if i.fetch_add(1, atomic::Ordering::SeqCst) % 100 == 99 {
            Err(io::Error::new(io::ErrorKind::Other, "worker closed"))
        } else {
            Ok(())
        }
    });
    let mut c = c.connect(None).unwrap();

    let start = time::Instant::now();
    for i in 0..num {
        if !c.run_one(0, &["default"]).unwrap() {
            num = i;
            break;
        }
    }
    let stop = start.elapsed();
    let stop_secs = stop.as_secs() * 1_000_000_000 + stop.subsec_nanos() as u64;
    let stop_secs = stop_secs as f64 / 1_000_000_000.0;

    println!(
        "Processed {} jobs in {:.2} seconds, rate: {} jobs/s\n",
        num,
        stop_secs,
        num as f64 / stop_secs,
    );
}
