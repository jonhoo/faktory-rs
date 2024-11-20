use super::Client;

impl Client {
    /*
    From Go bindings:

    // Move the given jobs from structure to the Dead set.
    // Faktory will not touch them anymore but you can still see them in the Web UI.
    //
    // Kill(Retries, OfType("DataSyncJob").WithJids("abc", "123"))
    Kill(name Structure, filter JobFilter) error

    // Move the given jobs to their associated queue so they can be immediately
    // picked up and processed.
    Requeue(name Structure, filter JobFilter) error

    // Throw away the given jobs, e.g. if you want to delete all jobs named "QuickbooksSyncJob"
    //
    //   Discard(Dead, OfType("QuickbooksSyncJob"))
    Discard(name Structure, filter JobFilter) error

    // Empty the entire given structure, e.g. if you want to clear all retries.
    // This is very fast as it is special cased by Faktory.
    Clear(name Structure) error
    */
}
