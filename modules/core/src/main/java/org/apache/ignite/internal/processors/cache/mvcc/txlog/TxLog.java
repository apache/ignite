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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;

/**
 *
 */
public class TxLog implements DbCheckpointListener {
    /** */
    public static final String TX_LOG_CACHE_NAME = "TxLog";

    /** */
    public static final int TX_LOG_CACHE_ID = CU.cacheId(TX_LOG_CACHE_NAME);

    /** */
    private static final TxKey LOWEST = new TxKey(0, 0);

    /** */
    private final IgniteCacheDatabaseSharedManager mgr;

    /** */
    private ReuseListImpl reuseList;

    /** */
    private TxLogTree tree;

    /** */
    private ConcurrentMap<TxKey, Sync> keyMap = new ConcurrentHashMap<>();

    /**
     *
     * @param ctx Kernal context.
     * @param mgr Database shared manager.
     */
    public TxLog(GridKernalContext ctx, IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        this.mgr = mgr;

        init(ctx);
    }

    /**
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException If failed.
     */
    private void init(GridKernalContext ctx) throws IgniteCheckedException {
        if (CU.isPersistenceEnabled(ctx.config())) {
            mgr.checkpointReadLock();

            try {
                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();
                PageMemoryEx pageMemory = (PageMemoryEx)mgr.dataRegion(TX_LOG_CACHE_NAME).pageMemory();

                long partMetaId = pageMemory.partitionMetaPageId(TX_LOG_CACHE_ID, 0);
                long partMetaPage = pageMemory.acquirePage(TX_LOG_CACHE_ID, partMetaId);

                long treeRoot, reuseListRoot;

                boolean isNew = false;

                try {
                    long pageAddr = pageMemory.writeLock(TX_LOG_CACHE_ID, partMetaId, partMetaPage);

                    try {
                        if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                            // Initialize new page.
                            PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                            io.initNewPage(pageAddr, partMetaId, pageMemory.pageSize());

                            treeRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, 0, PageMemory.FLAG_DATA);
                            reuseListRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, 0, PageMemory.FLAG_DATA);

                            assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                            assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;

                            io.setTreeRoot(pageAddr, treeRoot);
                            io.setReuseListRoot(pageAddr, reuseListRoot);

                            if (PageHandler.isWalDeltaRecordNeeded(pageMemory, TX_LOG_CACHE_ID, partMetaId, partMetaPage, wal, null))
                                wal.log(new MetaPageInitRecord(
                                    TX_LOG_CACHE_ID,
                                    partMetaId,
                                    io.getType(),
                                    io.getVersion(),
                                    treeRoot,
                                    reuseListRoot
                                ));

                            isNew = true;
                        }
                        else {
                            PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                            treeRoot = io.getTreeRoot(pageAddr);
                            reuseListRoot = io.getReuseListRoot(pageAddr);

                            assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA :
                                U.hexLong(treeRoot) + ", part=" + 0 + ", TX_LOG_CACHE_ID=" + TX_LOG_CACHE_ID;
                            assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA :
                                U.hexLong(reuseListRoot) + ", part=" + 0 + ", TX_LOG_CACHE_ID=" + TX_LOG_CACHE_ID;
                        }
                    }
                    finally {
                        pageMemory.writeUnlock(TX_LOG_CACHE_ID, partMetaId, partMetaPage, null, isNew);
                    }
                }
                finally {
                    pageMemory.releasePage(TX_LOG_CACHE_ID, partMetaId, partMetaPage);
                }

                reuseList = new ReuseListImpl(
                    TX_LOG_CACHE_ID,
                    TX_LOG_CACHE_NAME,
                    pageMemory,
                    wal,
                    reuseListRoot,
                    isNew);

                tree = new TxLogTree(pageMemory, wal, treeRoot, reuseList, ctx.failure(), isNew);

                ((GridCacheDatabaseSharedManager)mgr).addCheckpointListener(this);
            }
            finally {
                mgr.checkpointReadUnlock();
            }
        }
        else {
            PageMemory pageMemory = mgr.dataRegion(TX_LOG_CACHE_NAME).pageMemory();
            ReuseList reuseList1 = mgr.reuseList(TX_LOG_CACHE_NAME);

            long treeRoot;

            if ((treeRoot = reuseList1.takeRecycledPage()) == 0L)
                treeRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, INDEX_PARTITION, FLAG_IDX);

            tree = new TxLogTree(pageMemory, null, treeRoot, reuseList1, ctx.failure(), true);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        Executor executor = ctx.executor();

        if (executor == null)
            reuseList.saveMetadata();
        else {
            executor.execute(() -> {
                try {
                    reuseList.saveMetadata();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });
        }
    }

    /**
     *
     * @param major Major version.
     * @param minor Minor version.
     * @return Transaction state for given version.
     * @throws IgniteCheckedException If failed.
     */
    public byte get(long major, long minor) throws IgniteCheckedException {
        return get(new TxKey(major, minor));
    }

    /**
     *
     * @param key Transaction key.
     * @return Transaction state for given version.
     * @throws IgniteCheckedException If failed.
     */
    public byte get(TxKey key) throws IgniteCheckedException {
        TxRow row = tree.findOne(key);

        return row == null ? TxState.NA : row.state();
    }

    /**
     *
     * @param key TxKey.
     * @param state  Transaction state for given version.
     * @param primary Flag if this is a primary node.
     * @throws IgniteCheckedException If failed.
     */
    public void put(TxKey key, byte state, boolean primary) throws IgniteCheckedException {
        Sync sync = syncObject(key);

        try {
            mgr.checkpointReadLock();

            try {
                synchronized (sync) {
                    tree.invoke(key, null, new TxLogUpdateClosure(key.major(), key.minor(), state, primary));
                }
            }
            finally {
                mgr.checkpointReadUnlock();
            }
        } finally {
            evict(key, sync);
        }
    }

    /**
     * Removes all records less or equals to the given version.
     *
     * @param major Major version.
     * @param minor Minor version.
     * @throws IgniteCheckedException If failed.
     */
    public void removeUntil(long major, long minor) throws IgniteCheckedException {
        TraversingClosure clo = new TraversingClosure(major, minor);

        tree.iterate(LOWEST, clo, clo);

        if (clo.rows != null) {
            for (TxKey row : clo.rows) {
                remove(row);
            }
        }
    }

    /** */
    private void remove(TxKey key) throws IgniteCheckedException {
        Sync sync = syncObject(key);

        try {
            mgr.checkpointReadLock();

            try {
                synchronized (sync) {
                    tree.removex(key);
                }
            }
            finally {
                mgr.checkpointReadUnlock();
            }
        } finally {
            evict(key, sync);
        }
    }

    /** */
    private Sync syncObject(TxKey key) {
        Sync sync = keyMap.get(key);

        while (true) {
            if (sync == null) {
                Sync old = keyMap.putIfAbsent(key, sync = new Sync());

                if (old == null)
                    return sync;
                else
                    sync = old;
            }
            else {
                int cntr = sync.counter;

                while (cntr > 0) {
                    if (sync.casCounter(cntr, cntr + 1))
                        return sync;

                    cntr = sync.counter;
                }

                sync = keyMap.get(key);
            }
        }
    }

    /** */
    private void evict(TxKey key, Sync sync) {
        assert sync != null;

        int cntr = sync.counter;

        while (true) {
            assert cntr > 0;

            if (!sync.casCounter(cntr, cntr - 1)) {
                cntr = sync.counter;

                continue;
            }

            if (cntr == 1) {
                boolean removed = keyMap.remove(key, sync);

                assert removed;
            }

            break;
        }
    }

    /**
     *
     */
    private static class TraversingClosure extends TxKey implements BPlusTree.TreeRowClosure<TxKey, TxRow> {
        /** */
        private List<TxKey> rows;

        /**
         *
         * @param major Major version.
         * @param minor Minor version.
         */
        TraversingClosure(long major, long minor) {
            super(major, minor);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<TxKey, TxRow> tree, BPlusIO<TxKey> io, long pageAddr,
                                       int idx) throws IgniteCheckedException {

            if (rows == null)
                rows = new ArrayList<>();

            TxLogIO logIO = (TxLogIO)io;
            int offset = io.offset(idx);

            rows.add(new TxKey(logIO.getMajor(pageAddr, offset), logIO.getMinor(pageAddr, offset)));

            return true;
        }
    }

    /** */
    private static class Sync {
        /** */
        private static final AtomicIntegerFieldUpdater<Sync> UPD = AtomicIntegerFieldUpdater.newUpdater(Sync.class, "counter");

        /** */
        volatile int counter = 1;

        /** */
        boolean casCounter(int old, int upd) {
            return UPD.compareAndSet(this, old, upd);
        }
    }

    /**
     * TxLog update closure.
     */
    private static final class TxLogUpdateClosure implements IgniteTree.InvokeClosure<TxRow> {
        /** */
        private final long major;

        /** */
        private final long minor;

        /** */
        private final byte newState;

        /** */
        private final boolean primary;

        /** */
        private IgniteTree.OperationType treeOp;

        /**
         *
         * @param major Coordinator version.
         * @param minor Counter.
         * @param newState New Tx newState.
         * @param primary Flag if this is primary node.
         */
        TxLogUpdateClosure(long major, long minor, byte newState, boolean primary) {
            assert major > MVCC_CRD_COUNTER_NA && minor > MVCC_COUNTER_NA && newState != TxState.NA;
            this.major = major;
            this.minor = minor;
            this.newState = newState;
            this.primary = primary;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable TxRow row) {
            if (row == null) {
                valid();

                return;
            }

            byte currState = row.state();

            switch (currState) {
                case TxState.NA:
                    checkNa(currState);

                    break;

                case TxState.PREPARED:
                    checkPrepared(currState);

                    break;

                case TxState.COMMITTED:
                    checkCommitted(currState);

                    break;

                case TxState.ABORTED:
                    checkAborted(currState);

                    break;

                default:
                    throw new IllegalStateException("Unknown tx state: " + currState);
            }
        }

        /** {@inheritDoc} */
        @Override public TxRow newRow() {
            return treeOp == IgniteTree.OperationType.PUT ? new TxRow(major, minor, newState) : null;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /**
         * Checks update possibility for {@code TxState.NA} tx status.
         *
         * @param currState Current tx state.
         */
        private void checkNa(byte currState) {
            switch (newState) {
                case TxState.ABORTED:
                case TxState.PREPARED:
                    valid();

                    break;

                case TxState.COMMITTED:
                    invalid(currState); // TODO IGNITE-8445

                    break;

                default:
                    invalid(currState);
            }
        }

        /**
         * Checks update possibility for {@code TxState.PREPARED} status.
         *
         * @param currState Current tx state.
         */
        private void checkPrepared(byte currState) {
            switch (newState) {
                case TxState.ABORTED:
                case TxState.COMMITTED:
                    valid();

                    break;

                case TxState.PREPARED:
                    ignore();

                    break;

                default:
                    invalid(currState);
            }
        }

        /**
         * Checks update possibility for {@code TxState.COMMITTED} status.
         *
         * @param currState Current tx state.
         */
        private void checkCommitted(byte currState) {
            switch (newState) {
                case TxState.COMMITTED:
                    ignore();

                    break;

                case TxState.PREPARED:
                    if (primary)
                        ignore(); // In case when remote tx has updated the current state before.
                    else
                        invalid(currState);

                    break;

                default:
                    invalid(currState);
            }
        }

        /**
         * Checks update possibility for {@code TxState.ABORTED} status.
         *
         * @param currState Current tx state.
         */
        private void checkAborted(byte currState) {
            switch (newState) {
                case TxState.ABORTED:
                    ignore();

                    break;

                case TxState.PREPARED:
                    if (primary)
                        ignore(); // In case when remote tx has updated the current state before.
                    else
                        invalid(currState);

                    break;

                default:
                    invalid(currState);
            }
        }

        /**
         * Action for valid tx status update.
         */
        private void valid() {
            assert treeOp == null;

            treeOp = IgniteTree.OperationType.PUT;
        }

        /**
         * Action for invalid tx status update.
         */
        private void invalid(byte currState) {
            assert treeOp == null;

            throw new IllegalStateException("Unexpected new transaction state. [currState=" +
                currState +  ", newState=" + newState +  ", cntr=" + minor +']');
        }

        /**
         * Action for ignoring tx status update.
         */
        private void ignore() {
            assert treeOp == null;

            treeOp = IgniteTree.OperationType.NOOP;
        }
    }
}
