#[derive(Debug)]
pub(crate) struct System {
    sys: sysinfo::System,
    pid: sysinfo::Pid,
}

impl System {
    /// We are running tests on latest ubuntu, macos, and windows runners (see
    /// "test.yml" workflow) which account for the majority of use-cases;
    /// Linux, macOS, and Windows _are_ in the sysinfo's list of suported OSes:
    /// https://docs.rs/sysinfo/latest/sysinfo/index.html#supported-oses
    pub(crate) fn try_new() -> Result<Option<Self>, ()> {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return Ok(None);
        }
        let sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from(std::process::id() as usize);
        let mut system = Self { sys, pid };
        if system.refresh_current_process() {
            Ok(Some(system))
        } else {
            Err(())
        }
    }

    /// Refresh stats for the current process.
    ///
    /// This will refresh memory stats for the process with [`System::pid`],
    /// and return a `bool` indicating whether the operation succeded. It will
    /// result in `false` if there are any file system access permissions issues.
    ///
    /// This convenience method is used by [`System::rss_kb`] internally, but
    /// is also utilized as a probe of whether the process has got necessary
    /// permissions during [`System`] instantiation.
    pub(crate) fn refresh_current_process(&mut self) -> bool {
        self.sys.refresh_processes_specifics(
            // only current proccess ...
            sysinfo::ProcessesToUpdate::Some(&[self.pid]),
            true,
            // ... and only memory stats
            sysinfo::ProcessRefreshKind::nothing().with_memory(),
        ) == 1
    }

    pub(crate) fn rss_kb(&mut self) -> u64 {
        let ok = self.refresh_current_process();
        if !ok {
            // we've checked once during the system's instantiation and this failing
            // later on should be rare, but still let's at least not silence it
            tracing::warn!(
                PID = self.pid.as_u32(),
                "failed to refresh current process stats"
            );
        }
        let pstats = self.sys.process(self.pid);

        // the only safe way to obtain a system is using [`System::try_new`]
        // which is performing a probe of whether the process can access stats
        // files, and so we can _expect_ the data to be avalable here; there is
        // ofcouse a chance that something happened with the stats files during
        // the program's execution or reading from disk just failed, but then
        // `sysinfo` internally will just fallback to the last collected data;
        // to protect ourselves from unexpected changes in `sysinfo` crate, we
        // prefer to not unwrap here, we are also not bailing since the worker
        // can continue processing tasks
        if let Some(pstats) = pstats {
            let rss_bytes = pstats.memory();
            rss_bytes / 1024
        } else {
            tracing::warn!(
                PID = self.pid.as_u32(),
                "stats for current process not found"
            );
            0
        }
    }
}
