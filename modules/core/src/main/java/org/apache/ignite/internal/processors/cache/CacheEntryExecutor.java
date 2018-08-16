package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.StripedExecutor;
import org.jetbrains.annotations.Nullable;

public class CacheEntryExecutor {

    private final StripedExecutor executor;

    public CacheEntryExecutor(StripedExecutor executor) {
        this.executor = executor;
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

        executor.execute(entry.key().partition(), () -> {
            GridCacheEntryEx entry0 = entry;

            R result;

            for (;;) {
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
            }
        });

        return future;
    }

    static class ExecutionContext {

        private GridCacheEntryEx entry;

    }
}
