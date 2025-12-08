pub(crate) struct System {
    sys: sysinfo::System,
    pid: sysinfo::Pid,
}

impl System {
    /// We are running tests on latest ubuntu, macos, and windows runners (see
    /// "test.yml" workflow) which account for the majority of use-cases;
    /// Linux, macOS, and Windows _are_ in the sysinfo's list of suported OSes:
    /// https://docs.rs/sysinfo/latest/sysinfo/index.html#supported-oses
    pub(crate) fn new_if_os_supported() -> Option<Self> {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return None;
        }
        let sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from(std::process::id() as usize);
        Some(Self { sys, pid })
    }

    pub(crate) fn rss_kb(&mut self) -> u64 {
        self.sys.refresh_processes_specifics(
            // only current proccess ...
            sysinfo::ProcessesToUpdate::Some(&[self.pid]),
            true,
            // ... and only memory stats
            sysinfo::ProcessRefreshKind::nothing().with_memory(),
        );
        let pstats = self
            .sys
            .process(self.pid)
            .expect("current process to exist");
        let rss_bytes = pstats.memory();
        rss_bytes >> 10
    }
}
