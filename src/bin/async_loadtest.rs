use clap::value_parser;
use clap::{Arg, Command};
use faktory::*;
use rand::prelude::*;
use std::io;
use std::process;
use std::sync::{self, atomic};
use std::time;

const QUEUES: &[&str] = &["queue0", "queue1", "queue2", "queue3", "queue4"];

#[tokio::main]
async fn main() {
    let matches = Command::new("My Super Program (Async)")
        .version("0.1")
        .about("Benchmark the performance of Rust Faktory async consumers and producers")
        .arg(
            Arg::new("jobs")
                .help("Number of jobs to run")
                .value_parser(value_parser!(usize))
                .index(1)
                .default_value("30000"),
        )
        .arg(
            Arg::new("threads")
                .help("Number of consumers/producers to run")
                .value_parser(value_parser!(usize))
                .index(2)
                .default_value("10"),
        )
        .get_matches();

    let jobs: usize = *matches.get_one("jobs").expect("default_value is set");
    let threads: usize = *matches.get_one("threads").expect("default_value is set");
    println!(
        "Running loadtest with {} jobs and {} threads",
        jobs, threads
    );

    // ensure that we can actually connect to the server;
    // will create a client, run a handshake with Faktory,
    // and drop the cliet immediately afterwards;
    if let Err(e) = AsyncProducer::connect(None).await {
        println!("{}", e);
        process::exit(1);
    }

    let pushed = sync::Arc::new(atomic::AtomicUsize::new(0));
    let popped = sync::Arc::new(atomic::AtomicUsize::new(0));

    let start = time::Instant::now();
    let threads: Vec<tokio::task::JoinHandle<Result<_, Error>>> = (0..threads)
        .map(|_| {
            let pushed = sync::Arc::clone(&pushed);
            let popped = sync::Arc::clone(&popped);
            tokio::spawn(async move {
                // make producer and consumer
                let mut p = AsyncProducer::connect(None).await.unwrap();
                let mut c = AsyncConsumerBuilder::default();
                c.register("SomeJob", |_| {
                    Box::pin(async move {
                        let mut rng = rand::thread_rng();
                        if rng.gen_bool(0.01) {
                            Err(io::Error::new(io::ErrorKind::Other, "worker closed"))
                        } else {
                            Ok(())
                        }
                    })
                });
                let mut c = c.connect(None).await.unwrap();

                let mut rng = rand::rngs::OsRng;
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
                        p.enqueue(job).await?;
                        if pushed.fetch_add(1, atomic::Ordering::SeqCst) >= jobs {
                            return Ok(idx);
                        }
                    } else {
                        // pop
                        c.run_one(0, &random_queues[..]).await?;
                        if popped.fetch_add(1, atomic::Ordering::SeqCst) >= jobs {
                            return Ok(idx);
                        }
                    }
                }
                Ok(jobs)
            })
        })
        .collect();

    let mut _ops_count = Vec::with_capacity(threads.len());
    for jh in threads {
        _ops_count.push(jh.await.unwrap())
    }
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
    println!("{:?}", _ops_count);
}
