use clap::value_parser;
use clap::{Arg, Command};
use faktory::*;
use rand::prelude::*;
use std::io;
use std::process;
use std::sync::{self, atomic};
use std::time;
use tokio::task;

const QUEUES: &[&str] = &["queue0", "queue1", "queue2", "queue3", "queue4"];

#[tokio::main]
async fn main() {
    let matches = Command::new("My Super Program")
        .version("0.1")
        .about("Benchmark the performance of Rust Faktory async workers and client")
        .arg(
            Arg::new("jobs")
                .help("Number of jobs to run")
                .value_parser(value_parser!(usize))
                .index(1)
                .default_value("30000"),
        )
        .arg(
            Arg::new("threads")
                .help("Number of workers/clients to run")
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
    if let Err(e) = Client::connect(None).await {
        println!("{}", e);
        process::exit(1);
    }

    let pushed = sync::Arc::new(atomic::AtomicUsize::new(0));
    let popped = sync::Arc::new(atomic::AtomicUsize::new(0));

    let start = time::Instant::now();

    let mut set = task::JoinSet::new();
    for _ in 0..threads {
        let pushed = sync::Arc::clone(&pushed);
        let popped = sync::Arc::clone(&popped);
        set.spawn(async move {
            // make producer and consumer
            let mut p = Client::connect(None).await.unwrap();
            let mut c = WorkerBuilder::default();
            c.register_fn("SomeJob", |_| {
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
                        return Ok::<usize, Error>(idx);
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
        });
    }

    let mut ops_count = Vec::with_capacity(threads);
    while let Some(res) = set.join_next().await {
        ops_count.push(res.unwrap())
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
    println!(
        "Number of operations (pushes and pops) per thread: {:?}",
        ops_count
    );
}
