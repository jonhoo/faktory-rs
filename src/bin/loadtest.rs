use clap::ArgMatches;
use clap::{Arg, Command};
use faktory::{ConsumerBuilder, Error, Job, Producer};
use rand::prelude::*;
use std::io;
use std::process;
use std::sync::{self, atomic};
use std::thread;
use std::time;

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

struct LoadtestOptions {
    jobs: usize,
    threads: usize,
}

fn parse_command_line_args() -> ArgMatches {
    let parser = setup_parser();
    parser.get_matches()
}

fn get_opts(parse: Option<Box<dyn FnOnce() -> ArgMatches>>) -> LoadtestOptions {
    let matches = parse.unwrap_or(Box::new(parse_command_line_args))();
    let jobs = matches
        .get_one::<String>("jobs")
        .unwrap()
        .parse::<usize>()
        .expect("Number of jobs to run");
    let threads = matches
        .get_one::<String>("threads")
        .unwrap()
        .parse::<usize>()
        .expect("Number of consumers/producers to run");

    LoadtestOptions { jobs, threads }
}

#[derive(Clone, Default)]
struct AtomicCounter {
    value: sync::Arc<atomic::AtomicUsize>,
}

impl AtomicCounter {
    fn inc(&self) -> usize {
        self.value.fetch_add(1, atomic::Ordering::SeqCst)
    }

    fn get(&self) -> usize {
        self.value.load(atomic::Ordering::SeqCst)
    }
}

fn do_e2e_jobs(
    e2e_jobs_count: usize,
    jobs_produced_counter: AtomicCounter,
    jobs_consumed_counter: AtomicCounter,
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

    for idx in 0..e2e_jobs_count {
        if idx % 2 == 0 {
            let mut job = Job::new(
                "SomeJob",
                vec![serde_json::Value::from(1), "string".into(), 3.into()],
            );
            job.priority = Some(rng.gen_range(1..10));
            job.queue = QUEUES.choose(&mut rng).unwrap().to_string();
            p.enqueue(job)?;
            if jobs_produced_counter.inc() >= e2e_jobs_count {
                return Ok(idx);
            };
        } else {
            c.run_one(0, &random_queues[..])?;
            if jobs_consumed_counter.inc() >= e2e_jobs_count {
                return Ok(idx);
            }
        }
    }

    Ok(e2e_jobs_count)
}

fn load_with_jobs(
    jobs: usize,
    threads: usize,
) -> (Result<Vec<usize>, Error>, AtomicCounter, AtomicCounter) {
    let pushed = AtomicCounter::default();
    let popped = AtomicCounter::default();
    let threads: Vec<thread::JoinHandle<Result<_, Error>>> = (0..threads)
        .map(|_| {
            let pushed = pushed.clone();
            let popped = popped.clone();
            thread::spawn(move || do_e2e_jobs(jobs, pushed, popped))
        })
        .collect();

    let ops_per_thread = threads.into_iter().map(|jt| jt.join().unwrap()).collect();
    (ops_per_thread, pushed, popped)
}

fn calc_secs_elapsed(elapsed: &time::Duration) -> f64 {
    let elapsed_nanos = elapsed.as_secs() * 1_000_000_000 + u64::from(elapsed.subsec_nanos());
    elapsed_nanos as f64 / 1_000_000_000.0
}

fn run_loadtest(opts: &LoadtestOptions) {
    println!(
        "Running loadtest with {} jobs and {} threads",
        opts.jobs, opts.threads
    );

    ping!();

    let start = time::Instant::now();
    let (_ops_count, pushed, popped) = load_with_jobs(opts.jobs, opts.threads);
    let stop = calc_secs_elapsed(&start.elapsed());

    println!(
        "Processed {} pushes and {} pops in {:.2} seconds, rate: {} jobs/s",
        pushed.get(),
        popped.get(),
        stop,
        opts.jobs as f64 / stop,
    );
}

fn main() {
    let opts = get_opts(None);
    run_loadtest(&opts)
}

#[cfg(test)]
mod test {
    use super::{
        calc_secs_elapsed, do_e2e_jobs, get_opts, load_with_jobs, run_loadtest, setup_parser,
        AtomicCounter, DEFAULT_JOBS_COUNT, DEFAULT_THREADS_COUNT,
    };
    use clap::ArgMatches;
    use std::{env, ops::Add as _, time};

    macro_rules! skip_test_if_faktory_url_not_provided {
        () => {
            if env::var_os("FAKTORY_URL").is_none() {
                return;
            };
        };
    }

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
        assert_eq!(opts.jobs, DEFAULT_JOBS_COUNT.parse::<usize>().unwrap());
        assert_eq!(
            opts.threads,
            DEFAULT_THREADS_COUNT.parse::<usize>().unwrap()
        );
    }

    #[test]
    fn test_opts_parsed_correctly_when_args_provided() {
        let argv = ["./target/release/loadtest", "20000", "8"];
        let parse_fn = prepare_parse_fn(&argv);
        let opts = get_opts(Some(Box::new(parse_fn)));
        assert_eq!(opts.jobs, 20_000);
        assert_eq!(opts.threads, 8);
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
    fn test_do_e2e_jobs() {
        skip_test_if_faktory_url_not_provided!();
        let e2e_jobs_count = 10_000;
        let jobs_produced = AtomicCounter::default();
        let jobs_consumed = AtomicCounter::default();
        let _ = do_e2e_jobs(e2e_jobs_count, jobs_produced.clone(), jobs_consumed.clone());
        assert!(jobs_produced.get() == e2e_jobs_count / 2);
        assert!(jobs_consumed.get() == e2e_jobs_count / 2);
    }

    #[test]
    fn test_load_with_jobs() {
        skip_test_if_faktory_url_not_provided!();
        let e2e_jobs_count = 30_000;
        let (_, produced, consumed) = load_with_jobs(e2e_jobs_count, 10);
        assert!(produced.get() >= e2e_jobs_count);
        assert!(consumed.get() >= e2e_jobs_count);
    }

    #[test]
    fn test_loadtest_flow() {
        skip_test_if_faktory_url_not_provided!();
        let argv = ["./target/release/loadtest", "21000", "10"];
        let parse_fn = prepare_parse_fn(&argv);
        let opts = get_opts(Some(Box::new(parse_fn)));
        run_loadtest(&opts);
    }
}
