use faktory::Job;

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
