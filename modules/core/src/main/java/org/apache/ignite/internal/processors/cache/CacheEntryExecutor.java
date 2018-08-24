package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.StripedExecutor;

public class CacheEntryExecutor {
    private static final boolean TPP_ENABLED = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_ENABLE_THREAD_PER_PARTITION, true);

    private final StripedExecutor executor;

    private final GridCacheSharedContext cctx;

    public CacheEntryExecutor(StripedExecutor executor, GridCacheSharedContext cctx) {
        this.executor = executor;
        this.cctx = cctx;
    }

    public void stop(boolean cancel) {
        executor.stop();
    }

    public <R> CacheEntryOperationFuture<R> execute(
        int partitionId,
        CacheEntryOperation<R> operationClojure
    ) {
        assert partitionId >= 0 : "Entry partition is undefined [partId=" + partitionId + "]";

        CacheEntryOperationFuture<R> future = new CacheEntryOperationFuture<>();

        Runnable cmd = () -> {
            R result;

            for (;;) {
                cctx.database().checkpointReadLock();

                try {
                    result = operationClojure.invoke();

                    future.onDone(result);

                    break;
                }
                catch (Throwable e) {
                    future.onDone(e);

                    break;
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }
            }
        };

        if (TPP_ENABLED)
            executor.execute(partitionId, cmd);
        else
            cmd.run();

        return future;
    }
}
