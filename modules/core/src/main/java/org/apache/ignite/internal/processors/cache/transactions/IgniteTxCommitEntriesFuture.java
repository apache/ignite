package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

public class IgniteTxCommitEntriesFuture extends GridCompoundFuture<GridCacheUpdateTxResult, Boolean> {

    public static final IgniteTxCommitEntriesFuture FINISHED = finished();

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
