use std::time::Duration;
use testcontainers::core::ImageExt as _;
use testcontainers::core::IntoContainerPort as _;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;

#[macro_export]
macro_rules! skip_check {
    () => {
        if std::env::var_os("FAKTORY_URL").is_none() {
            return;
        }
    };
}

#[macro_export]
macro_rules! skip_if_not_enterprise {
    () => {
        if std::env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
    };
}

#[macro_export]
macro_rules! assert_gt {
    ($a:expr, $b:expr $(, $rest:expr) *) => {
        assert!($a > $b $(, $rest) *)
    };
}

#[macro_export]
macro_rules! assert_gte {
    ($a:expr, $b:expr $(, $rest:expr) *) => {
        assert!($a >= $b $(, $rest) *)
    };
}

#[macro_export]
macro_rules! assert_lt {
    ($a:expr, $b:expr $(, $rest:expr) *) => {
        assert!($a < $b $(, $rest) *)
    };
}

#[macro_export]
macro_rules! assert_lte {
    ($a:expr, $b:expr $(, $rest:expr) *) => {
        assert!($a <= $b $(, $rest) *)
    };
}

#[cfg(feature = "ent")]
pub fn learn_faktory_url() -> String {
    let url = std::env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    url.to_str().expect("Is a utf-8 string").to_owned()
}

pub struct TestContext {
    _container_handle: ContainerAsync<GenericImage>,
    pub faktory_url: String,
}

pub async fn setup(container_name: Option<String>) -> TestContext {
    let container_request = GenericImage::new("contribsys/faktory", "1.9.1")
        .with_exposed_port(7419.tcp())
        .with_exposed_port(7420.tcp())
        .with_entrypoint("/faktory")
        .with_wait_for(WaitFor::message_on_stdout("listening at :7419"))
        .with_cmd(["-b", ":7419", "-w", ":7420"])
        .with_startup_timeout(Duration::from_secs(5));
    let container_request = match container_name {
        None => container_request,
        Some(name) => container_request
            .with_reuse(testcontainers::ReuseDirective::Always)
            .with_container_name(name),
    };
    let faktory_container = container_request
        .start()
        .await
        .expect("launched container with Faktory just fine");
    let port = faktory_container
        .ports()
        .await
        .expect("post to have been published")
        .map_to_host_port_ipv4(7419)
        .expect("host post to have been assigned by OS");
    let faktory_url = format!("tcp://localhost:{}", port);
    TestContext {
        _container_handle: faktory_container,
        faktory_url,
    }
}
