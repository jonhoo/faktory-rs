extern crate faktory;

use faktory::{AsyncProducer, JobBuilder};

use crate::skip_if_not_enterprise;
use crate::utils::learn_faktory_url;

#[tokio::test]
async fn async_enqueue_expiring_job() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(
        JobBuilder::new("order")
            .expires_at(chrono::Utc::now() + chrono::Duration::seconds(3))
            .build(),
    )
    .await
    .unwrap();
}
