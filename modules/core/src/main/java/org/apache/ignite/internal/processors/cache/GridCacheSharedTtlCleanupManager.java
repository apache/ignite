/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Periodically removes expired entities from caches with {@link CacheConfiguration#isEagerTtl()} flag set.
 */
public class GridCacheSharedTtlCleanupManager extends GridCacheSharedManagerAdapter {
    /** Ttl cleanup worker thread sleep interval, ms. */
    private static final long CLEANUP_WORKER_SLEEP_INTERVAL = 500;

    /** Limit of expired entries processed by worker for certain cache in one pass. */
    private static final int CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT = 1000;

    /** Cleanup worker. */
    private CleanupWorker cleanupWorker;

    /** Lock on worker thread creation. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Map of registered ttl managers, where the cache id is used as the key. */
    private final Map<Integer, GridCacheTtlManager> mgrs = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        lock.lock();

        try {
            stopCleanupWorker();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Register ttl manager of cache for periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void register(GridCacheTtlManager mgr) {
        lock.lock();

        try {
            if (cleanupWorker == null)
                startCleanupWorker();

            mgrs.put(mgr.context().cacheId(), mgr);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Unregister ttl manager of cache from periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void unregister(GridCacheTtlManager mgr) {
        lock.lock();

        try {
            mgrs.remove(mgr.context().cacheId());

            if (mgrs.isEmpty())
                stopCleanupWorker();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return {@code True} if eager TTL is enabled.
     */
    public boolean eagerTtlEnabled() {
        assert cctx != null : "Manager is not started";

        lock.lock();

        try {
            return cleanupWorker != null;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     *
     */
    private void startCleanupWorker() {
        assert lock.isHeldByCurrentThread() : "The lock must be held by the current thread.";

        cleanupWorker = new CleanupWorker();

        new IgniteThread(cleanupWorker).start();
    }

    /**
     *
     */
    private void stopCleanupWorker() {
        assert lock.isHeldByCurrentThread() : "The lock must be held by the current thread.";

        if (null != cleanupWorker) {
            U.cancel(cleanupWorker);
            U.join(cleanupWorker, log);

            cleanupWorker = null;
        }
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker extends GridWorker {
        /**
         * Creates cleanup worker.
         */
        CleanupWorker() {
            super(cctx.igniteInstanceName(), "ttl-cleanup-worker", cctx.logger(GridCacheSharedTtlCleanupManager.class),
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                blockingSectionBegin();

                try {
                    cctx.discovery().localJoin();
                }
                finally {
                    blockingSectionEnd();
                }

                assert !cctx.kernalContext().recoveryMode();

                while (!isCancelled()) {
                    boolean expiredRemains = false;

                    Map<Integer, GridCacheTtlManager> cp;

                    lock.lockInterruptibly();

                    try {
                        cp = new HashMap<>(mgrs);
                    }
                    finally {
                        lock.unlock();
                    }

                    for (Map.Entry<Integer, GridCacheTtlManager> mgr : cp.entrySet()) {
                        lock.lockInterruptibly();

                        try {
                            if (!mgrs.containsKey(mgr.getKey()))
                                continue;

                            updateHeartbeat();

                            if (mgr.getValue().expire(CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT))
                                expiredRemains = true;

                            if (isCancelled())
                                return;
                        }
                        finally {
                            lock.unlock();
                        }
                    }

                    updateHeartbeat();

                    if (!expiredRemains)
                        U.sleep(CLEANUP_WORKER_SLEEP_INTERVAL);

                    onIdle();
                }
            }
            catch (Throwable t) {
                if (X.hasCause(t, NodeStoppingException.class)) {
                    isCancelled = true; // Treat node stopping as valid worker cancellation.

                    return;
                }

                if (!(t instanceof IgniteInterruptedCheckedException || t instanceof InterruptedException))
                    err = t;

                throw t;
            }
            finally {
                if (err == null && !isCancelled)
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }
    }
}
