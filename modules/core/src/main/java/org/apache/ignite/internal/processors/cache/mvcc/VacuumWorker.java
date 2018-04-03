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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewMvccVersionFast;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * Vacuum worker.
 */
public class VacuumWorker  extends GridWorker {
    /** Count of rows, being processed within a single checkpoint lock. */
    private static final int BATCH_SIZE = 1000;

    /** */
    private static int workerCntr;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final BlockingQueue<VacuumTask> cleanupQueue;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     * @param cleanupQueue Cleanup tasks queue.
     */
    VacuumWorker(GridKernalContext ctx, IgniteLogger log, BlockingQueue<VacuumTask> cleanupQueue) {
        super(ctx.igniteInstanceName(), "vacuum-cleaner-cache-id=" + workerCntr++, log);

        this.ctx = ctx;
        this.cleanupQueue = cleanupQueue;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        while (!isCancelled()) {
            VacuumTask task = cleanupQueue.take();

            MvccVersion cleanupVer = task.cleanupVer();
            GridDhtLocalPartition part = task.part();
            GridFutureAdapter<VacuumMetrics> fut = task.future();

            try {
                VacuumMetrics metrics = processPartition(part, cleanupVer);

                fut.onDone(metrics);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
            catch (IgniteCheckedException e) {
                ctx.coordinators().setVacuumError(e);

                U.error(log, "Error occurred during vacuum cleanup [partition=" + part + ", cleanupVer=" +
                    cleanupVer + ']', e);

                fut.onDone(e);
            }
            catch (Throwable e) {
                ctx.coordinators().setVacuumError(e);

                fut.onDone(e);

                throw e;
            }
        }
    }

    /**
     * Process partition.
     *
     * @param part Partition.
     * @param cleanupVer Cleanup version.
     * @return Number of cleaned rows.
     * @throws IgniteCheckedException If failed.
     */
    private VacuumMetrics processPartition(GridDhtLocalPartition part,
        MvccVersion cleanupVer) throws IgniteCheckedException {
        long startNanoTime = System.nanoTime();

        boolean reserved = false;

        if (part != null && part.state() != EVICTED)
            reserved = part.state() == OWNING && part.reserve();

        if (!reserved)
            return new VacuumMetrics();

        boolean cpLocked = false;

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

            VacuumMetrics metrics = new VacuumMetrics();
            GridCacheContext cctx = null;
            boolean shared = part.group().sharedGroup();

            int prevCacheId = shared ? CU.UNDEFINED_CACHE_ID : part.group().groupId();

            if (!shared)
                cctx = ctx.cache().context().cacheContext(part.group().groupId());

            KeyCacheObject prevKey = null;
            List<MvccLinkAwareSearchRow> cleanupRows = new ArrayList<>();
            long cntr = 0;
            MvccDataRow row;
            MvccLinkAwareSearchRow cleanupRow = null;
            int prevHash = 0;
            boolean first = true;

            do {
                row = cursor.next() ? (MvccDataRow)cursor.get() : null;

                assert prevKey != null || first;
                assert prevCacheId != CU.UNDEFINED_CACHE_ID || !shared || first;
                assert row == null || row.mvccCounter() > MVCC_COUNTER_NA && row.mvccCoordinatorVersion() > 0;

                boolean needCleanup;

                if (row != null) {
                    boolean cacheChanged = shared && row.cacheId() != prevCacheId;

                    needCleanup = !first && (row.hash() != prevHash || cacheChanged || !prevKey.equals(row.key()));

                    if (compare(row, cleanupVer) <= 0 && hasNewMvccVersionFast(row) &&
                        MvccUtils.compareNewVersion(row, cleanupVer) <= 0 &&
                        ctx.coordinators().state(row.newMvccCoordinatorVersion(), row.newMvccCounter()) == TxState.COMMITTED) {
                        cleanupRow = new MvccLinkAwareSearchRow(row.cacheId(), row.key(), row.mvccCoordinatorVersion(),
                            row.mvccCounter(), row.mvccOperationCounter(), row.link());
                    }

                    metrics.addScannedRowsCount(1);
                }
                else
                    needCleanup = true;

                if (!cleanupRows.isEmpty() && needCleanup) {
                    long cleanupStartNanoTime = System.nanoTime();

                    if (!cpLocked) {
                        ctx.cache().context().database().checkpointReadLock();

                        cpLocked = true;
                    }

                    if (shared)
                        cctx = ctx.cache().context().cacheContext(prevCacheId);

                    assert cctx != null;
                    assert cleanupRows.get(0).key().equals(prevKey);
                    assert shared ? cleanupRows.get(0).cacheId() == cctx.cacheId() : cleanupRows.get(0).cacheId() == CU.UNDEFINED_CACHE_ID;

                    GridCacheEntryEx entry = cctx.cache().entryEx(prevKey);

                    while (true) {
                        entry.lockEntry();

                        if (!entry.obsolete())
                            break;

                        entry.unlockEntry();
                    }

                    try {
                        part.dataStore().cleanup(cctx, cleanupRows);

                        cleanupRows.clear();

                        if (++cntr % BATCH_SIZE == 0) {
                            ctx.cache().context().database().checkpointReadUnlock();

                            cpLocked = false;
                        }
                    }
                    finally {
                        entry.unlockEntry();

                        cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);

                        metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                        metrics.addCleanupRowsCnt(cleanupRows.size());
                    }
                }

                if (cleanupRow != null) {
                    cleanupRows.add(cleanupRow);

                    cleanupRow = null;
                }

                if (row != null) {
                    prevKey = row.key();
                    prevHash = row.hash();

                    if (shared)
                        prevCacheId = row.cacheId();
                }

                first = false;
            }
            while (row != null && !isCancelled());

            metrics.addSearchNanoTime(System.nanoTime() - startNanoTime - metrics.cleanupNanoTime());

            return metrics;
        }
        finally {
            if (cpLocked)
                ctx.cache().context().database().checkpointReadUnlock();

            part.release();
        }
    }
}
