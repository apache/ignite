package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.StripedExecutor;
import org.jetbrains.annotations.Nullable;

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
        GridCacheEntryEx entry,
        CacheEntryOperation<R> operationClojure,
        @Nullable CacheEntryRefresh refreshClojure,
        @Nullable CacheEntryOperationCallback<R> callbackClojure
    ) {
        assert entry.key() != null : "Entry key is null [entry=" + entry + "]";
        assert entry.key().partition() != -1 : "Entry partition is undefined [entry=" + entry + "]";

        CacheEntryOperationFuture<R> future = new CacheEntryOperationFuture<>();

        Runnable cmd = () -> {
            GridCacheEntryEx entry0 = entry;

            R result;

            for (;;) {
                cctx.database().checkpointReadLock();

                try {
                    result = operationClojure.invoke(entry0);

                    if (callbackClojure != null)
                        callbackClojure.invoke(entry0, result);

                    future.onDone(result);

                    break;
                }
                catch (GridCacheEntryRemovedException re) {
                    if (refreshClojure != null) {
                        try {
                            entry0 = refreshClojure.refresh(entry0);

                            assert entry0 != null;
                        }
                        catch (IgniteCheckedException e) {
                            future.onDone(e);

                            break;
                        }
                    }
                    else {
                        future.onDone(re);

                        break;
                    }
                }
                catch (IgniteCheckedException e) {
                    future.onDone(e);

                    break;
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }
            }
        };

        if (TPP_ENABLED)
            executor.execute(entry.key().partition(), cmd);
        else
            cmd.run();

        return future;
    }
}
