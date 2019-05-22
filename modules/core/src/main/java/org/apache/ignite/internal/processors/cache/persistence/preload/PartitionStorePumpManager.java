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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgnitePartitionCatchUpLog;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
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
public class PartitionStorePumpManager {
    /** */
    private final IgniteLogger log;

    /** */
    private final Object mux = new Object();

    /** The list of cache data storages to sync their states. */
    private final Queue<T2<GridFutureAdapter<Boolean>, IgnitePartitionCatchUpLog>> catchQueue =
        new ConcurrentLinkedQueue<>();

    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /** */
    private StoragePumpWorker pumpWorker;

    /**
     * @param ktx Kernel context.
     */
    public PartitionStorePumpManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config());

        log = ktx.log(PartitionStorePumpManager.class);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    void start0(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        this.cctx = cctx;
    }

    /**
     * @param cancel Cancel flag.
     */
    void stop0(boolean cancel) {
        synchronized (mux) {
            stopStorePumpWorker();
        }
    }

    /**
     * @param src The src to start work with.
     * @return The future completes when the log has been catched.
     */
    public GridFutureAdapter<Boolean> registerPumpSource(IgnitePartitionCatchUpLog src) {
        synchronized (mux) {
            GridFutureAdapter<Boolean> fut0 = new GridFutureAdapter<>();

            fut0.listen(future -> {
                U.log(log,"All entries applyed to the original store: " + src);
            });

            catchQueue.add(new T2<>(fut0, src));

            if (pumpWorker == null)
                startStorePumpWorker();

            return fut0;
        }
    }

    /**
     * @param src The src instance to stop handling.
     */
    public void unregisterPumpSource(IgnitePartitionCatchUpLog src) {
        synchronized (mux) {
            if (catchQueue.isEmpty())
                stopStorePumpWorker();
        }
    }

    /**
     * Start cache data store pump worker thread.
     */
    private void startStorePumpWorker() {
        pumpWorker = new StoragePumpWorker();

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
    private class StoragePumpWorker extends GridWorker {
        /** */
        public StoragePumpWorker() {
            super(cctx.igniteInstanceName(),
                "rebalance-store-pump-worker",
                cctx.kernalContext().log(PartitionStorePumpManager.class),
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            T2<GridFutureAdapter<Boolean>, IgnitePartitionCatchUpLog> tup = null;

            try {
                assert !cctx.kernalContext().recoveryMode();
                assert CU.isPersistenceEnabled(cctx.kernalContext().config());

                while ((tup = catchQueue.poll()) != null && !isCancelled()) {
                    updateHeartbeat();

                    IgnitePartitionCatchUpLog catchLog = tup.get2();

                    U.log(log, "Start applying cache entries to the original store: " + catchLog);

                    WALIterator iter = catchLog.replay();

                    ((GridCacheDatabaseSharedManager)cctx.database()).applyUpdates(iter,
                        (ptr, rec) -> true,
                        (entry) -> true,
                        true);

                    assert catchLog.catched();

                    tup.get1().onDone(true);

                    unregisterPumpSource(catchLog);
                }
            }
            catch (IgniteCheckedException e) {
                log.error("The pump worker has an error during processing storage temporary entries", e);

                if (tup != null)
                    tup.get1().onDone(e);

                err = e;
            }
            catch (Throwable t) {
                if (!(X.hasCause(t, IgniteInterruptedCheckedException.class, InterruptedException.class)))
                    err = t;

                if (tup != null)
                    tup.get1().onDone(t);

                throw t;
            }
            finally {
                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }
    }
}
