#[macro_export]
macro_rules! skip_check {
    () => {
        if std::env::var_os("FAKTORY_URL").is_none() {
            return;
        }
    };
}