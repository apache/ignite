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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.state;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * Vacuum worker.
 */
public class VacuumWorker extends GridWorker {
    /** */
    private final BlockingQueue<VacuumTask> cleanupQueue;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     * @param cleanupQueue Cleanup tasks queue.
     */
    VacuumWorker(GridKernalContext ctx, IgniteLogger log, BlockingQueue<VacuumTask> cleanupQueue) {
        super(ctx.igniteInstanceName(), "vacuum-cleaner", log);

        this.cleanupQueue = cleanupQueue;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        while (!isCancelled()) {
            VacuumTask task = cleanupQueue.take();

            try {
                task.onDone(processPartition(task));
            }
            catch (IgniteInterruptedCheckedException e) {
                throw e; // Cancelled.
            }
            catch (Throwable e) {
                task.onDone(e);

                if (e instanceof Error)
                    throw (Error) e;
            }
        }
    }

    /**
     * Process partition.
     *
     * @param task VacuumTask.
     * @throws IgniteCheckedException If failed.
     */
    private VacuumMetrics processPartition(VacuumTask task) throws IgniteCheckedException {
        long startNanoTime = System.nanoTime();

        GridDhtLocalPartition part = task.part();

        VacuumMetrics metrics = new VacuumMetrics();

        if (part == null || part.state() != OWNING || !part.reserve())
            return metrics;

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

            KeyCacheObject prevKey = null; Object rest = null;

            List<MvccLinkAwareSearchRow> cleanupRows = null;

            MvccSnapshot snapshot = task.snapshot();

            GridCacheContext cctx = null; int curCacheId = CU.UNDEFINED_CACHE_ID;

            boolean shared = part.group().sharedGroup();

            if (!shared)
                cctx = part.group().singleCacheContext();

            while (cursor.next()) {
                if (isCancelled())
                    throw new IgniteInterruptedCheckedException("Operation has been cancelled.");

                MvccDataRow row = (MvccDataRow)cursor.get();

                if (prevKey == null)
                    prevKey = row.key();

                if (cctx == null) {
                    assert shared;

                    curCacheId = row.cacheId();
                    cctx = part.group().shared().cacheContext(curCacheId);
                }

                if (!prevKey.equals(row.key()) || (shared && curCacheId != row.cacheId())) {
                    if (rest != null || !F.isEmpty(cleanupRows))
                        cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

                    cleanupRows = null; rest = null;

                    if (shared && curCacheId != row.cacheId())
                        cctx = part.group().shared().cacheContext(curCacheId = row.cacheId());

                    prevKey = row.key();
                }

                if (canClean(row, snapshot, cctx))
                    cleanupRows = addRow(cleanupRows, row);
                else if (actualize(cctx, row, snapshot))
                    rest = addRest(rest, row);

                metrics.addScannedRowsCount(1);
            }

            if (rest != null || !F.isEmpty(cleanupRows))
                cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

            metrics.addSearchNanoTime(System.nanoTime() - startNanoTime - metrics.cleanupNanoTime());

            return metrics;
        }
        finally {
            part.release();
        }
    }

    /** */
    @SuppressWarnings("unchecked")
    @NotNull private Object addRest(@Nullable Object rest, MvccDataRow row) {
        if (rest == null)
            rest = row;
        else if (rest.getClass() == ArrayList.class)
            ((List)rest).add(row);
        else {
            ArrayList list = new ArrayList();

            list.add(rest);
            list.add(row);

            rest = list;
        }

        return rest;
    }

    /**
     * @param rows Collection of rows.
     * @param row Row to add.
     * @return Collection of rows.
     */
    @NotNull private List<MvccLinkAwareSearchRow> addRow(@Nullable List<MvccLinkAwareSearchRow> rows, MvccDataRow row) {
        if (rows == null)
            rows = new ArrayList<>();

        rows.add(new MvccLinkAwareSearchRow(row.cacheId(), row.key(), row.mvccCoordinatorVersion(),
            row.mvccCounter(), row.mvccOperationCounter(), row.link()));

        return rows;
    }

    /**
     * @param row Mvcc row to check.
     * @param snapshot Cleanup version to compare with.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    private boolean canClean(MvccDataRow row, MvccSnapshot snapshot,
        GridCacheContext cctx) throws IgniteCheckedException {
        // Row can be safely cleaned if it has ABORTED min version or COMMITTED and less than cleanup one max version.
        return compare(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
            && hasNewVersion(row) && MvccUtils.compareNewVersion(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
            && state(cctx, row.newMvccCoordinatorVersion(), row.newMvccCounter(),
            row.newMvccOperationCounter() | (row.newMvccTxState() << PageIO.MVCC_HINTS_BIT_OFF)) == TxState.COMMITTED
            || state(cctx, row.mvccCoordinatorVersion(), row.mvccCounter(),
            row.mvccOperationCounter() | (row.mvccTxState() << PageIO.MVCC_HINTS_BIT_OFF)) == TxState.ABORTED;
    }

    /** */
    private boolean actualize(GridCacheContext cctx, MvccDataRow row,
        MvccSnapshot snapshot) throws IgniteCheckedException {
        return isVisible(cctx, snapshot, row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter(), false)
            && (row.mvccTxState() == TxState.NA || (row.newMvccCoordinatorVersion() != MVCC_CRD_COUNTER_NA && row.newMvccTxState() == TxState.NA));
    }

    /**
     *
     * @param part Local partition.
     * @param key Key.
     * @param cleanupRows Cleanup rows.
     * @param cctx Cache context.
     * @param metrics Vacuum metrics.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void cleanup(GridDhtLocalPartition part, KeyCacheObject key, List<MvccLinkAwareSearchRow> cleanupRows,
        Object rest, GridCacheContext cctx, VacuumMetrics metrics) throws IgniteCheckedException {
        assert key != null && cctx != null && (!F.isEmpty(cleanupRows) || rest != null);

        long cleanupStartNanoTime = System.nanoTime();

        GridCacheEntryEx entry = cctx.cache().entryEx(key);

        while (true) {
            entry.lockEntry();

            if (!entry.obsolete())
                break;

            entry.unlockEntry();

            entry = cctx.cache().entryEx(key);
        }

        cctx.shared().database().checkpointReadLock();

        int cleaned = 0;

        try {
            if (cleanupRows != null)
                cleaned = part.dataStore().cleanup(cctx, cleanupRows);

            if (rest != null) {
                if (rest.getClass() == ArrayList.class) {
                    for (MvccDataRow row : ((List<MvccDataRow>)rest)) {
                        part.dataStore().updateTxState(cctx, row);
                    }
                }
                else
                    part.dataStore().updateTxState(cctx, (MvccDataRow)rest);
            }
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();

            entry.unlockEntry();
            cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);

            metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
            metrics.addCleanupRowsCnt(cleaned);
        }
    }
}
