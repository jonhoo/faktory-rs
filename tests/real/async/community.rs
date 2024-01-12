extern crate faktory;

use faktory::AsyncProducer;

use crate::skip_check;

#[tokio::test]
async fn async_hello_p() {
    skip_check!();
    let p = AsyncProducer::connect(None).await.unwrap();
    drop(p);
}
