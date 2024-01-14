extern crate faktory;

use faktory::{AsyncConsumerBuilder, AsyncProducer, Job, JobBuilder};

use crate::skip_check;

#[tokio::test]
async fn async_hello_p() {
    skip_check!();
    let p = AsyncProducer::connect(None).await.unwrap();
    drop(p);
}

#[tokio::test]
async fn async_enqueue_job() {
    skip_check!();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(JobBuilder::new("order").build()).await.unwrap();
}

async fn process_order(j: Job) -> Result<(), std::io::Error> {
    println!("{:?}", j);
    assert_eq!(j.kind(), "order");
    Ok(())
}

#[tokio::test]
async fn roundtrip_async() {
    skip_check!();
    let jid = String::from("x-job-id-0123456782");
    let mut c = AsyncConsumerBuilder::default();
    c.register("order", |job| Box::pin(process_order(job)));
    c.register("image", |job| {
        Box::pin(async move {
            println!("{:?}", job);
            assert_eq!(job.kind(), "image");
            Ok(())
        })
    });
    let mut c = c.connect(None).await.unwrap();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(
        JobBuilder::new("order")
            .jid(&jid)
            .args(vec!["ISBN-13:9781718501850"])
            .queue("roundtrip_async")
            .build(),
    )
    .await
    .unwrap();
    let had_one = c.run_one(0, &["roundtrip_async"]).await.unwrap();
    assert!(had_one);

    let drained = !c.run_one(0, &["roundtrip_async"]).await.unwrap();
    assert!(drained);
}
