extern crate faktory;

use std::env;

fn main() {
    let num = env::args()
        .skip(1)
        .next()
        .and_then(|v| i64::from_str_radix(&*v, 10).ok());
    if num.is_none() {
        eprintln!("usage: push N");
        return;
    }
    let num = num.unwrap();

    use std::thread;
    use std::time;
    thread::sleep(time::Duration::from_millis(300));
    let mut p = faktory::Producer::connect(None).unwrap();

    let start = time::Instant::now();
    for i in 0..num {
        use faktory::Job;
        p.enqueue(Job::new("bench", vec![i])).unwrap();
    }
    let stop = start.elapsed();
    let stop_secs = stop.as_secs() * 1_000_000_000 + stop.subsec_nanos() as u64;
    let stop_secs = stop_secs as f64 / 1_000_000_000.0;

    println!(
        "Enqueued {} jobs in {:.2} seconds, rate: {} jobs/s\n",
        num,
        stop_secs,
        num as f64 / stop_secs,
    );
}
