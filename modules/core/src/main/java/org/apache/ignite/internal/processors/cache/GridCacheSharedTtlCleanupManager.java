/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
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

    /** Mutex on worker thread creation. */
    private final Object mux = new Object();

    /** List of registered ttl managers. */
    private List<GridCacheTtlManager> mgrs = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        synchronized (mux) {
            stopCleanupWorker();
        }
    }

    /**
     * Register ttl manager of cache for periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void register(GridCacheTtlManager mgr) {
        synchronized (mux) {
            if (cleanupWorker == null)
                startCleanupWorker();

            mgrs.add(mgr);
        }
    }

    /**
     * Unregister ttl manager of cache from periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void unregister(GridCacheTtlManager mgr) {
        synchronized (mux) {
            mgrs.remove(mgr);

            if (mgrs.isEmpty())
                stopCleanupWorker();
        }
    }

    /**
     * @return {@code True} if eager TTL is enabled.
     */
    public boolean eagerTtlEnabled() {
        assert cctx != null : "Manager is not started";

        return cleanupWorker != null;
    }

    /**
     *
     */
    private void startCleanupWorker() {
        cleanupWorker = new CleanupWorker();

        new IgniteThread(cleanupWorker).start();
    }

    /**
     *
     */
    private void stopCleanupWorker() {
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
                while (!isCancelled()) {
                    boolean expiredRemains = false;

                    for (GridCacheTtlManager mgr : mgrs) {
                        updateHeartbeat();

                        if (mgr.expire(CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT))
                            expiredRemains = true;

                        if (isCancelled())
                            return;
                    }

                    updateHeartbeat();

                    if (!expiredRemains)
                        U.sleep(CLEANUP_WORKER_SLEEP_INTERVAL);

                    onIdle();
                }
            }
            catch (Throwable t) {
                if (!(t instanceof IgniteInterruptedCheckedException))
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
