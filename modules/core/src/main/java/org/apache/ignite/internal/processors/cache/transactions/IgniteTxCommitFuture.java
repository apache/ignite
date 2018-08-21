package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.util.future.GridFutureAdapter;

public class IgniteTxCommitFuture extends GridFutureAdapter<Boolean> {
    private IgniteTxCommitEntriesFuture commitEntriesFuture;

    private boolean started;

    private boolean async;

    public IgniteTxCommitFuture(IgniteTxCommitEntriesFuture commitEntriesFuture, boolean async) {
        this.commitEntriesFuture = commitEntriesFuture;
        this.async = async;
        this.started = true;

        if (!async)
            onDone();
    }

    public IgniteTxCommitFuture(Throwable err) {
        onDone(err);

        started = false;
    }

    public IgniteTxCommitFuture(boolean started) {
        onDone();

        this.started = started;
    }

    public IgniteTxCommitEntriesFuture commitEntriesFuture() {
        return commitEntriesFuture;
    }

    public boolean async() {
        return async;
    }

    public boolean started() {
        return started;
    }
}
