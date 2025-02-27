use crate::mutate::{Filter, JobSet};
use crate::{
    proto::single::{MutationAction, MutationType},
    Client, Error, JobId,
};
use std::borrow::Borrow;

#[cfg(doc)]
use crate::JobBuilder;

impl Client {
    /// Re-enqueue the jobs.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Internally, the Faktory server is keeping scheduled, failed, and dead jobs
    /// in dedicated sets (rather than queues). This method allows to immediately move
    /// jobs from the targeted (see [`Target`]) set back to the queues those jobs
    /// are destined for. This will apply to the jobs satisfying the [`filter`](crate::mutate::Filter).
    ///
    /// Say, we got a job which we purposefully delayed with [`JobBuilder::at`].
    /// Internally, the Faktory server will be keeping this job in a special set
    /// of `scheduled` jobs. If we now need to force-schedule this job, we can
    /// achieve this like so:
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use faktory::{JobId, Client};
    /// # use faktory::mutate::{JobSet, Filter};
    /// # let mut client = Client::connect().await.unwrap();
    /// let job_id1 = JobId::new("3sgE_qwtqw1501");
    /// let job_id2 = JobId::new("3sgE_qwtqw1502");
    /// let scheduled = [&job_id1, &job_id2];
    /// let filter = Filter::builder().jids(scheduled.as_slice()).build();
    /// client.requeue(JobSet::Scheduled, &filter).await.unwrap();
    /// # });
    /// ```
    /// In the example above we used jobs' IDS to find the jobs we need in
    /// the targeted set, but there also other options - see [`Filter`].
    pub async fn requeue<'a, F>(&mut self, target: JobSet, filter: F) -> Result<(), Error>
    where
        F: Borrow<Filter<'a>>,
    {
        self.mutate(MutationType::Requeue, target, Some(filter.borrow()))
            .await
    }

    /// Re-enqueue the jobs with the given ids.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Similar to [`Client::requeue`], but will create a filter (see [`Filter`])
    /// with the given `jids` for you.
    pub async fn requeue_by_ids(
        &mut self,
        target: JobSet,
        jids: &'_ [&'_ JobId],
    ) -> Result<(), Error> {
        let filter = Filter::builder().jids(jids).build();
        self.mutate(MutationType::Requeue, target, Some(&filter))
            .await
    }

    /// Discard the jobs.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Will throw the jobs away without any chance for re-scheduling
    /// on the server side. If you want to still be able to process the jobs,
    /// use [`Client::kill`] instead.
    ///
    /// E.g. to discard the currently enqueued jobs having "fizz" argument:
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use faktory::Client;
    /// # use faktory::mutate::{JobSet, Filter};
    /// # let mut client = Client::connect().await.unwrap();
    /// let filter = Filter::builder()
    ///     .pattern(r#"*\"args\":\[\"fizz\"\]*"#)
    ///     .build();
    /// client.discard(JobSet::Scheduled, &filter).await.unwrap();
    /// # });
    /// ```
    pub async fn discard<'a, F>(&mut self, target: JobSet, filter: F) -> Result<(), Error>
    where
        F: Borrow<Filter<'a>>,
    {
        self.mutate(MutationType::Discard, target, Some(filter.borrow()))
            .await
    }

    /// Discard the jobs with the given ids.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Similar to [`Client::discard`], but will create a filter (see [`Filter`])
    /// with the given `jids` for you.
    pub async fn discard_by_ids(
        &mut self,
        target: JobSet,
        jids: &'_ [&'_ JobId],
    ) -> Result<(), Error> {
        let filter = Filter::builder().jids(jids).build();
        self.mutate(MutationType::Discard, target, Some(&filter))
            .await
    }

    /// Kill a set of jobs.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Moves the jobs from the target structure to the `dead` set, meaning Faktory
    /// will not touch it further unless you ask it to do so. You then can, for example,
    /// manually process those jobs via the Web UI or send another mutation command
    /// targeting [`JobSet::Dead`] set.
    ///
    /// E.g. to kill the currently enqueued jobs with "bill" argument:
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use faktory::Client;
    /// # use faktory::mutate::{JobSet, Filter};
    /// # let mut client = Client::connect().await.unwrap();
    /// let filter = Filter::builder()
    ///     .pattern(r#"*\"args\":\[\"bill\"\]*"#)
    ///     .build();
    /// client.kill(JobSet::Scheduled, &filter).await.unwrap();
    /// # });
    /// ```
    pub async fn kill<'a, F>(&mut self, target: JobSet, filter: F) -> Result<(), Error>
    where
        F: Borrow<Filter<'a>>,
    {
        self.mutate(MutationType::Kill, target, Some(filter.borrow()))
            .await
    }

    /// Kill the jobs with the given ids.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Similar to [`Client::kill`], but will create a filter (see [`Filter`])
    /// with the given `jids` for you.
    pub async fn kill_by_ids(
        &mut self,
        target: JobSet,
        jids: &'_ [&'_ JobId],
    ) -> Result<(), Error> {
        let filter = Filter::builder().jids(jids).build();
        self.mutate(MutationType::Kill, target, Some(&filter)).await
    }

    /// Purge the targeted structure.
    ///
    /// ***Warning!*** The `MUTATE` API is not supposed to be used as part of application logic,
    /// you will want to use it for administration purposes only.
    ///
    /// Will have the same effect as [`Client::discard`] with an empty [`Filter`],
    /// but is special cased by Faktory and so is performed faster. Can be thought of as
    /// `TRUNCATE tablename` operation in the SQL world versus `DELETE FROM tablename`.
    ///
    /// E.g. to purge all the jobs that are pending in the `reties` set:
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use faktory::Client;
    /// # use faktory::mutate::JobSet;
    /// # let mut client = Client::connect().await.unwrap();
    /// client.clear(JobSet::Retries).await.unwrap();
    /// # });
    /// ```
    pub async fn clear(&mut self, target: JobSet) -> Result<(), Error> {
        self.mutate(MutationType::Clear, target, None).await
    }

    // For reference: https://github.com/contribsys/faktory/blob/10ccc2270dc2a1c95c3583f7c291a51b0292bb62/server/mutate.go#L35-L59
    // The faktory will pull the targeted set from Redis to it's memory, iterate over each stringified job
    // looking for a substring "id":"..." or performing regexp search, then deserialize the matches into Jobs and
    // perform the action (e.g. requeue).
    async fn mutate(
        &mut self,
        mtype: MutationType,
        mtarget: JobSet,
        mfilter: Option<&'_ Filter<'_>>,
    ) -> Result<(), Error> {
        self.issue(&MutationAction {
            cmd: mtype,
            target: mtarget,
            filter: mfilter,
        })
        .await?
        .read_ok()
        .await
    }
}
