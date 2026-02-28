use crate::skip_check;
use faktory::{Job, JobBuilder, JobId, WorkerBuilder, WorkerId};
use std::{io, sync};

#[tokio::test(flavor = "multi_thread")]
async fn test_pool() {
    skip_check!();

    let local = "bb8_pool_roundtrip";

    // Set up a connection manager and pool
    let manager = faktory::bb8::ClientConnectionManager::new(
        "tcp://localhost:7419",
        faktory::bb8::TlsConnector::NoTls,
    );
    let pool: faktory::bb8::PooledClient = bb8::Pool::builder()
        .max_size(5)
        .build(manager)
        .await
        .unwrap();

    // Prepare jobs to enqueue
    let job1 = JobBuilder::new("pool_task")
        .jid(JobId::new("pool-job-001"))
        .args(vec!["test-data-1"])
        .queue(local)
        .build();

    let job2 = JobBuilder::new("pool_task")
        .jid(JobId::new("pool-job-002"))
        .args(vec!["test-data-2"])
        .queue(local)
        .build();

    // Get connections from pool and enqueue jobs
    {
        let mut client_conn = pool.get().await.unwrap();
        client_conn.enqueue(job1).await.unwrap();
    }

    {
        let mut client_conn = pool.get().await.unwrap();
        client_conn.enqueue(job2).await.unwrap();
    }

    // Verify pool can handle concurrent operations
    let enqueue_tasks = (0..3).map(|i| {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut client = pool.get().await.unwrap();
            let job = JobBuilder::new("pool_task")
                .jid(JobId::new(&format!("concurrent-{:02}", i)))
                .args(vec![format!("concurrent-data-{}", i)])
                .queue(local)
                .build();
            client.enqueue(job).await.unwrap();
        })
    });

    // Wait for all concurrent enqueue operations
    for task in enqueue_tasks {
        task.await.unwrap();
    }

    // Set up worker to consume jobs
    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut worker = WorkerBuilder::default()
        .hostname("bb8-tester".to_string())
        .wid(WorkerId::new("bb8-pool-test"))
        .register_fn("pool_task", move |job| {
            let tx = sync::Arc::clone(&tx);
            Box::pin(async move {
                tx.lock().unwrap().send(job).unwrap();
                Ok::<(), io::Error>(())
            })
        })
        .connect()
        .await
        .unwrap();

    // Consume all 5 jobs (2 initial + 3 concurrent)
    let mut processed_jobs = Vec::new();
    for _ in 0..5 {
        let had_job = worker.run_one(0, &[local]).await.unwrap();
        assert!(had_job);
        let job = rx.recv().unwrap();
        processed_jobs.push(job);
    }

    // Verify no more jobs in queue
    let had_job = worker.run_one(0, &[local]).await.unwrap();
    assert!(!had_job);

    // Verify all jobs were processed correctly
    assert_eq!(processed_jobs.len(), 5);

    // Check that we have the expected job IDs
    let mut job_ids: Vec<String> = processed_jobs.iter().map(|j| j.id().to_string()).collect();
    job_ids.sort();

    let mut expected_ids = vec![
        "pool-job-001".to_string(),
        "pool-job-002".to_string(),
        "concurrent-00".to_string(),
        "concurrent-01".to_string(),
        "concurrent-02".to_string(),
    ];
    expected_ids.sort();

    assert_eq!(job_ids, expected_ids);

    // Verify all jobs have correct queue and kind
    for job in &processed_jobs {
        assert_eq!(job.queue, local);
        assert_eq!(job.kind(), "pool_task");
    }

    // Test pool connection reuse with server info
    {
        let mut client1 = pool.get().await.unwrap();
        let info1 = client1.current_info().await.unwrap();
        drop(client1);

        let mut client2 = pool.get().await.unwrap();
        let info2 = client2.current_info().await.unwrap();

        // Both connections should work and return valid server info
        assert_eq!(info1.server.version.major, info2.server.version.major);
        assert_eq!(info1.server.version.minor, info2.server.version.minor);
    }

    // Clean up the queue
    let mut cleanup_client = pool.get().await.unwrap();
    cleanup_client.queue_remove(&[local]).await.unwrap();
}
