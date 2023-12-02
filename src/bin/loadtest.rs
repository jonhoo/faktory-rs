use clap::ArgMatches;
use clap::{Arg, Command};
use faktory::{ConsumerBuilder, Error, Job, Producer};
use rand::prelude::*;
use std::collections::HashMap;
use std::io;
use std::process;
use std::sync::{self, atomic};
use std::thread;
use std::time;

type Counter = sync::Arc<atomic::AtomicUsize>;

const QUEUES: &[&str] = &["queue0", "queue1", "queue2", "queue3", "queue4"];
const DEFAULT_JOBS_COUNT: &str = "30000";
const DEFAULT_THREADS_COUNT: &str = "10";

macro_rules! ping {
    () => {
        if let Err(e) = Producer::connect(None) {
            println!(
                "{}. Failed to connect to \"Faktory\" service. Ensure it is running...",
                e
            );
            process::exit(1);
        }
    };
}

fn setup_parser() -> Command<'static> {
    Command::new("My Super Program")
        .version("0.1")
        .about("Benchmark the performance of Rust Faktory consumers and producers")
        .arg(
            Arg::new("jobs")
                .help("Number of jobs to run")
                .index(1)
                .default_value(DEFAULT_JOBS_COUNT)
                .takes_value(true),
        )
        .arg(
            Arg::new("threads")
                .help("Number of consumers/producers to run")
                .index(2)
                .default_value(DEFAULT_THREADS_COUNT)
                .takes_value(true),
        )
}

fn parse_command_line_args() -> ArgMatches {
    let parser = setup_parser();
    parser.get_matches()
}

fn get_opts(parse: Option<Box<dyn FnOnce() -> ArgMatches>>) -> HashMap<&'static str, usize> {
    let matches = parse.unwrap_or(Box::new(parse_command_line_args))();
    let jobs_count = matches
        .get_one::<String>("jobs")
        .unwrap()
        .parse::<usize>()
        .expect("Number of jobs to run");
    let threads_count = matches
        .get_one::<String>("threads")
        .unwrap()
        .parse::<usize>()
        .expect("Number of consumers/producers to run");

    let mut opts: HashMap<&'static str, usize> = HashMap::new();
    opts.insert("jobs", jobs_count);
    opts.insert("threads", threads_count);
    opts
}

fn get_new_counter(initial: usize) -> Counter {
    sync::Arc::new(atomic::AtomicUsize::new(initial))
}

fn clone_counter(counter: &Counter) -> Counter {
    sync::Arc::clone(counter)
}

fn count_up(counter: &Counter, val: usize) -> usize {
    counter.fetch_add(val, atomic::Ordering::SeqCst)
}

fn ask_counter(counter: &Counter) -> usize {
    counter.load(atomic::Ordering::SeqCst)
}

fn do_jobs_and_report(
    jobs_total_count: usize,
    jobs_produced_counter: Counter,
    jobs_consumed_counter: Counter,
) -> Result<usize, Error> {
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

    for idx in 0..jobs_total_count {
        if idx % 2 == 0 {
            let mut job = Job::new(
                "SomeJob",
                vec![serde_json::Value::from(1), "string".into(), 3.into()],
            );
            job.priority = Some(rng.gen_range(1..10));
            job.queue = QUEUES.choose(&mut rng).unwrap().to_string();
            p.enqueue(job)?;
            count_up(&jobs_produced_counter, 1);
        } else {
            c.run_one(0, &random_queues[..])?;
            count_up(&jobs_consumed_counter, 1);
        }
    }

    Ok(jobs_total_count)
}

fn calc_secs_elapsed(elapsed: &time::Duration) -> f64 {
    let elapsed_nanos = elapsed.as_secs() * 1_000_000_000 + u64::from(elapsed.subsec_nanos());
    elapsed_nanos as f64 / 1_000_000_000.0
}

fn main() {
    let opts = get_opts(None);
    let jobs_count = opts.get("jobs").unwrap().to_owned();
    let threads_count = opts.get("threads").unwrap().to_owned();
    println!(
        "Running loadtest with {} jobs and {} threads",
        jobs_count, threads_count
    );

    ping!();

    let pushed = get_new_counter(0);
    let popped = get_new_counter(0);
    let start = time::Instant::now();
    let threads: Vec<thread::JoinHandle<Result<_, Error>>> = (0..threads_count)
        .map(|_| {
            let pushed = clone_counter(&pushed);
            let popped = clone_counter(&popped);
            thread::spawn(move || do_jobs_and_report(jobs_count, pushed, popped))
        })
        .collect();

    let _ops_count: Result<Vec<_>, _> = threads.into_iter().map(|jt| jt.join().unwrap()).collect();
    let stop = calc_secs_elapsed(&start.elapsed());

    println!(
        "Processed {} pushes and {} pops in {:.2} seconds, rate: {} jobs/s",
        ask_counter(&pushed),
        ask_counter(&popped),
        stop,
        jobs_count as f64 / stop,
    );
}

#[cfg(test)]
mod test {
    use super::{
        ask_counter, calc_secs_elapsed, clone_counter, do_jobs_and_report, get_new_counter,
        get_opts, setup_parser, DEFAULT_JOBS_COUNT, DEFAULT_THREADS_COUNT,
    };
    use clap::ArgMatches;
    use std::{env, ops::Add as _, time};

    fn parse_command_line_args_mock(argv: &[&str]) -> ArgMatches {
        let cmd = setup_parser();
        cmd.try_get_matches_from(argv).unwrap()
    }

    fn prepare_parse_fn(argv: &[&str]) -> impl FnOnce() -> ArgMatches {
        let opts = parse_command_line_args_mock(argv);
        return move || opts;
    }

    #[test]
    #[should_panic(expected = "Number of jobs to run: ParseIntError { kind: InvalidDigit }")]
    fn test_invalid_number_of_jobs_provided() {
        let argv = ["./target/release/loadtest", "30k", "5"];
        let parse_fn = prepare_parse_fn(&argv);
        let _ = get_opts(Some(Box::new(parse_fn)));
    }

    #[test]
    #[should_panic(
        expected = "Number of consumers/producers to run: ParseIntError { kind: InvalidDigit }"
    )]
    fn test_invalid_number_of_threads_provided() {
        let argv = ["./target/release/loadtest", "30000", "five"];
        let parse_fn = prepare_parse_fn(&argv);
        let _ = get_opts(Some(Box::new(parse_fn)));
    }

    #[test]
    fn test_opts_fallbacks_when_args_not_provided() {
        let argv = ["./target/release/loadtest"];
        let parse_fn = prepare_parse_fn(&argv);
        let opts = get_opts(Some(Box::new(parse_fn)));
        let jobs_count = opts.get("jobs").unwrap().to_owned();
        let threads_count = opts.get("threads").unwrap().to_owned();
        assert_eq!(jobs_count, DEFAULT_JOBS_COUNT.parse::<usize>().unwrap());
        assert_eq!(
            threads_count,
            DEFAULT_THREADS_COUNT.parse::<usize>().unwrap()
        );
    }

    #[test]
    fn test_opts_parsed_correctly_when_args_provided() {
        let argv = ["./target/release/loadtest", "20000", "8"];
        let parse_fn = prepare_parse_fn(&argv);
        let opts = get_opts(Some(Box::new(parse_fn)));
        let jobs_count = opts.get("jobs").unwrap().to_owned();
        let threads_count = opts.get("threads").unwrap().to_owned();
        assert_eq!(jobs_count, 20_000);
        assert_eq!(threads_count, 8);
    }

    #[test]
    fn test_elapsed_time_calculated_correctly_for_loadtest() {
        let start = time::Instant::now();
        let stop = start.add(time::Duration::from_millis(1));
        let elapsed = stop - start;
        let res = calc_secs_elapsed(&elapsed);
        assert!(elapsed.as_secs() as f64 <= res);
    }

    #[test]
    fn test_do_jobs_and_report() {
        if env::var_os("FAKTORY_URL").is_none() {
            return;
        }
        let total_jobs_count = 10_000;
        let jobs_produced = get_new_counter(0);
        let jobs_consumed = get_new_counter(0);
        let _ = do_jobs_and_report(
            total_jobs_count,
            clone_counter(&jobs_produced),
            clone_counter(&jobs_consumed),
        );
        assert!(ask_counter(&jobs_produced) >= total_jobs_count / 2);
        assert!(ask_counter(&jobs_consumed) >= total_jobs_count / 2);
    }
}
