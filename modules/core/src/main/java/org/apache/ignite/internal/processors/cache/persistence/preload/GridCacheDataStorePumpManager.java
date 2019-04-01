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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgnitePartitionCatchUpLog;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 *
 */
public class GridCacheDataStorePumpManager extends GridCacheSharedManagerAdapter {
    /** */
    private final Object mux = new Object();

    /** The list of cache data storages to sync their states. */
    private final Queue<IgnitePartitionCatchUpLog> walStores = new ConcurrentLinkedQueue<>();

    /** */
    private StorePumpWorker pumpWorker;

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        synchronized (mux) {
            stopStorePumpWorker();
        }
    }

    /**
     * @param storage The storage to start work with.
     */
    public void startLogPush(IgnitePartitionCatchUpLog storage) {
        synchronized (mux) {
            if (pumpWorker == null)
                startStorePumpWorker();
        }

        walStores.add(storage);
    }

    /**
     * @param storage The storage instance to stop handling.
     */
    public void stopLogPush(IgnitePartitionCatchUpLog storage) {
        synchronized (mux) {
            if (walStores.isEmpty())
                stopStorePumpWorker();
        }
    }

    /**
     * Start cache data store pump worker thread.
     */
    private void startStorePumpWorker() {
        pumpWorker = new StorePumpWorker();

        new IgniteThread(pumpWorker).start();
    }

    /**
     * Stop cache data store pump worker thread.
     */
    private void stopStorePumpWorker() {
        if (pumpWorker != null) {
            U.cancel(pumpWorker);
            U.join(pumpWorker, log);

            pumpWorker = null;
        }
    }

    /**
     *
     */
    private class StorePumpWorker extends GridWorker {
        /** */
        public StorePumpWorker() {
            super(cctx.igniteInstanceName(),
                "rebalance-store-pump-worker",
                cctx.kernalContext().log(GridCacheDataStorePumpManager.class),
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                assert !cctx.kernalContext().recoveryMode();
                assert CU.isPersistenceEnabled(cctx.kernalContext().config());

                IgnitePartitionCatchUpLog catchLog;

                while ((catchLog = walStores.poll()) != null && !isCancelled()) {
                    updateHeartbeat();

                    WALIterator iter = catchLog.replay();

                    ((GridCacheDatabaseSharedManager)cctx.database()).applyUpdates(iter,
                        (ptr, rec) -> true,
                        (entry) -> true);

                    assert catchLog.catched();
                }

                onIdle();
            }
            catch (IgniteCheckedException e) {
                log.error("An error during processing storage temporary entries", e);
            }
            catch (Throwable t) {
                if (!(X.hasCause(t, IgniteInterruptedCheckedException.class, InterruptedException.class)))
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
