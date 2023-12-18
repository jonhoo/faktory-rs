use clap::{Arg, Command};
use faktory::*;
use rand::prelude::*;
use std::io;
use std::process;
use std::sync::{self, atomic};
use std::thread;
use std::time;

const QUEUES: &[&str] = &["queue0", "queue1", "queue2", "queue3", "queue4"];

fn main() {
    let matches = Command::new("My Super Program")
        .version("0.1")
        .about("Benchmark the performance of Rust Faktory consumers and producers")
        .arg(
            Arg::new("jobs")
                .help("Number of jobs to run")
                .index(1)
                .default_value("30000")
                .takes_value(true),
        )
        .arg(
            Arg::new("threads")
                .help("Number of consumers/producers to run")
                .index(2)
                .default_value("10")
                .takes_value(true),
        )
        .get_matches();

    let jobs = matches.value_of_t_or_exit::<usize>("jobs");
    let threads = matches.value_of_t_or_exit::<usize>("threads");
    println!(
        "Running loadtest with {} jobs and {} threads",
        jobs, threads
    );

    // ensure that we can actually connect to the server
    if let Err(e) = Producer::connect(None) {
        println!("{}", e);
        process::exit(1);
    }

    let pushed = sync::Arc::new(atomic::AtomicUsize::new(0));
    let popped = sync::Arc::new(atomic::AtomicUsize::new(0));

    let start = time::Instant::now();
    let threads: Vec<thread::JoinHandle<Result<_, Error>>> = (0..threads)
        .map(|_| {
            let pushed = sync::Arc::clone(&pushed);
            let popped = sync::Arc::clone(&popped);
            thread::spawn(move || {
                // make producer and consumer
                let mut p = Producer::connect(None).unwrap();
                let mut c = ConsumerBuilder::default();
                c.register("SomeJob", |_| {
                    let mut rng = rand::thread_rng();
                    if rng.gen_bool(0.01) {
                        Err(io::Error::new(io::ErrorKind::Other, "worker closed"))
                    } else {
                        Ok(())
                    }
                });
                let mut c = c.connect(None).unwrap();

                let mut rng = rand::thread_rng();
                let mut random_queues = Vec::from(QUEUES);
                random_queues.shuffle(&mut rng);
                for idx in 0..jobs {
                    if idx % 2 == 0 {
                        // push
                        let mut job = Job::new(
                            "SomeJob",
                            vec![serde_json::Value::from(1), "string".into(), 3.into()],
                        );
                        job.priority = Some(rng.gen_range(1..10));
                        job.queue = QUEUES.choose(&mut rng).unwrap().to_string();
                        p.enqueue(job)?;
                        if pushed.fetch_add(1, atomic::Ordering::SeqCst) >= jobs {
                            return Ok(idx);
                        }
                    } else {
                        // pop
                        c.run_one(0, &random_queues[..])?;
                        if popped.fetch_add(1, atomic::Ordering::SeqCst) >= jobs {
                            return Ok(idx);
                        }
                    }
                }
                Ok(jobs)
            })
        })
        .collect();

    let _ops_count: Result<Vec<_>, _> = threads.into_iter().map(|jt| jt.join().unwrap()).collect();
    let stop = start.elapsed();
    let stop_secs = stop.as_secs() * 1_000_000_000 + u64::from(stop.subsec_nanos());
    let stop_secs = stop_secs as f64 / 1_000_000_000.0;
    println!(
        "Processed {} pushes and {} pops in {:.2} seconds, rate: {} jobs/s",
        pushed.load(atomic::Ordering::SeqCst),
        popped.load(atomic::Ordering::SeqCst),
        stop_secs,
        jobs as f64 / stop_secs,
    );
    // println!("{:?}", _ops_count);
}
