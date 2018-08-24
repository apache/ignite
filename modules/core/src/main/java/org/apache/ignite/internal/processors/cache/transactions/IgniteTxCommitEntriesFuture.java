package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

public class IgniteTxCommitEntriesFuture extends GridCompoundFuture<GridCacheUpdateTxResult, Boolean> {

    public static final IgniteTxCommitEntriesFuture FINISHED = finished();

    public static final IgniteTxCommitEntriesFuture EMPTY = finished();

    private static IgniteTxCommitEntriesFuture empty() {
        IgniteTxCommitEntriesFuture fut = new IgniteTxCommitEntriesFuture();

        fut.markInitialized();

        fut.onDone();

        return fut;
    }

    public static IgniteTxCommitEntriesFuture finished() {
        IgniteTxCommitEntriesFuture fut = new IgniteTxCommitEntriesFuture();

        fut.onDone();

        return fut;
    }

    public static IgniteTxCommitEntriesFuture finishedWithError(Throwable t) {
        IgniteTxCommitEntriesFuture fut = new IgniteTxCommitEntriesFuture();

        fut.onDone(t);

        return fut;
    }
}
