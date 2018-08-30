/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxSelectForUpdateFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract future processing transaction enlisting and locking
 * of entries produced with DML and SELECT FOR UPDATE queries.
 */
public abstract class GridDhtTxAbstractEnlistFuture extends GridCacheFutureAdapter<Long>
    implements DhtLockFuture<Long> {
    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxAbstractEnlistFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxAbstractEnlistFuture.class, "done");

    /** SkipCntr field updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxAbstractEnlistFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxAbstractEnlistFuture.class, "skipCntr");

    /** Marker object. */
    private static final Object FINISHED = new Object();

    /** */
    private static final int BATCH_SIZE = 1024;

    /** In-flight batches per node limit. */
    private static final int BATCHES_PER_NODE = 5;

    /** */
    private static final int FIRST_BATCH_ID = 0;

    /** Future ID. */
    protected final IgniteUuid futId;

    /** Cache registry. */
    @GridToStringExclude
    protected final GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /** Thread. */
    protected final long threadId;

    /** Future ID. */
    protected final IgniteUuid nearFutId;

    /** Future ID. */
    protected final int nearMiniId;

    /** Partitions. */
    protected final int[] parts;

    /** Transaction. */
    protected final GridDhtTxLocalAdapter tx;

    /** Lock version. */
    protected final GridCacheVersion lockVer;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Processed entries count. */
    protected long cnt;

    /** Near node ID. */
    protected final UUID nearNodeId;

    /** Near lock version. */
    protected final GridCacheVersion nearLockVer;

    /** Timeout object. */
    @GridToStringExclude
    protected LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    protected final long timeout;

    /** Query iterator */
    private UpdateSourceIterator<?> it;

    /** Row extracted from iterator but not yet used. */
    private Object peek;

    /** */
    @SuppressWarnings({"FieldCanBeLocal"})
    @GridToStringExclude
    private volatile int skipCntr;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int done;

    /** */
    @GridToStringExclude
    private int batchIdCntr;

    /** Batches for sending to remote nodes. */
    private Map<UUID, Batch> batches;

    /** Batches already sent to remotes, but their acks are not received yet. */
    private ConcurrentMap<UUID, ConcurrentMap<Integer, Batch>> pending;

    /** */
    private WALPointer walPtr;

    /** Do not send DHT requests to near node. */
    protected boolean skipNearNodeUpdates;

    /** There are keys belonging to backup partitions on near node. */
    protected boolean hasNearNodeUpdates;

    /** Moving partitions. */
    private Map<Integer, Boolean> movingParts;

    /** Update counters to be sent to the near node in case it is a backup node also. */
    protected GridLongList nearUpdCntrs;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param parts Partitions.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    protected GridDhtTxAbstractEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        @Nullable int[] parts,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx) {
        assert tx != null;
        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;
        assert threadId == tx.threadId();

        this.threadId = threadId;
        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.timeout = timeout;
        this.tx = tx;
        this.parts = parts;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxAbstractEnlistFuture.class);
    }

    /**
     * @return iterator.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract UpdateSourceIterator<?> createIterator() throws IgniteCheckedException;

    /**
     *
     */
    public void init() {
        if (timeout < 0) {
            // Time is out.
            onDone(timeoutException());

            return;
        }
        else if (timeout > 0)
            timeoutObj = new LockTimeoutObject();

        while(true) {
            IgniteInternalFuture<?> fut = tx.lockFut;

            if (fut == GridDhtTxLocalAdapter.ROLLBACK_FUT) {
                onDone(tx.timedOut() ? tx.timeoutException() : tx.rollbackException());

                return;
            }
            else if (fut != null) {
                // Wait for previous future.
                assert fut instanceof GridNearTxAbstractEnlistFuture
                    || fut instanceof GridDhtTxAbstractEnlistFuture
                    || fut instanceof CompoundLockFuture
                    || fut instanceof GridNearTxSelectForUpdateFuture : fut;

                // Terminate this future if parent future is terminated by rollback.
                if (!fut.isDone()) {
                    fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                        @Override public void apply(IgniteInternalFuture fut) {
                            if (fut.error() != null)
                                onDone(fut.error());
                        }
                    });
                }
                else if (fut.error() != null)
                    onDone(fut.error());

                break;
            }
            else if (tx.updateLockFuture(null, this))
                break;
        }

        boolean added = cctx.mvcc().addFuture(this, futId);

        assert added;

        if (isDone()) {
            cctx.mvcc().removeFuture(futId);

            return;
        }

        if (timeoutObj != null)
            cctx.time().addTimeoutObject(timeoutObj);

        try {
            checkPartitions(parts);

            UpdateSourceIterator<?> it = createIterator();

            if (!it.hasNext()) {
                U.close(it, log);

                onDone(0L);

                return;
            }

            tx.addActiveCache(cctx, false);

            this.it = it;
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        continueLoop(false);
    }

    /**
     * Clears lock future.
     */
    protected void clearLockFuture() {
        tx.clearLockFuture(this);
    }

    /**
     * Iterates over iterator, applies changes locally and sends it on backups.
     *
     * @param ignoreCntr {@code True} if need to ignore skip counter.
     */
    private void continueLoop(boolean ignoreCntr) {
        if (isDone() || (!ignoreCntr && (SKIP_UPD.getAndIncrement(this) != 0)))
            return;

        GridDhtCacheAdapter cache = cctx.dhtCache();
        EnlistOperation op = it.operation();
        AffinityTopologyVersion topVer = tx.topologyVersionSnapshot();

        try {
            while (true) {
                while (hasNext0()) {
                    Object cur = next0();

                    KeyCacheObject key = cctx.toCacheKeyObject(op.isDeleteOrLock() ? cur : ((IgniteBiTuple)cur).getKey());

                    if (!ensureFreeSlot(key)) {
                        // Can't advance further at the moment.
                        peek = cur;

                        it.beforeDetach();

                        break;
                    }

                    GridDhtCacheEntry entry = cache.entryExx(key);

                    if (log.isDebugEnabled())
                        log.debug("Adding entry: " + entry);

                    assert !entry.detached();

                    CacheObject val = op.isDeleteOrLock() ? null : cctx.toCacheObject(((IgniteBiTuple)cur).getValue());

                    tx.markQueryEnlisted(mvccSnapshot);

                    GridCacheUpdateTxResult res;

                    while (true) {
                        cctx.shared().database().checkpointReadLock();

                        try {
                            switch (op) {
                                case DELETE:
                                    res = entry.mvccRemove(
                                        tx,
                                        cctx.localNodeId(),
                                        topVer,
                                        null,
                                        mvccSnapshot,
                                        isMoving(key.partition()));

                                    break;

                                case INSERT:
                                case UPSERT:
                                case UPDATE:
                                    res = entry.mvccSet(
                                        tx,
                                        cctx.localNodeId(),
                                        val,
                                        0,
                                        topVer,
                                        null,
                                        mvccSnapshot,
                                        op.cacheOperation(),
                                        isMoving(key.partition()),
                                        op.noCreate());

                                    break;

                                case LOCK:
                                    res = entry.mvccLock(
                                        tx,
                                        mvccSnapshot);

                                    break;

                                default:
                                    throw new IgniteSQLException("Cannot acquire lock for operation [op= " + op + "]" +
                                        "Operation is unsupported at the moment ", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            entry = cache.entryExx(entry.key(), topVer);
                        }
                        finally {
                            cctx.shared().database().checkpointReadUnlock();
                        }
                    }

                    IgniteInternalFuture<GridCacheUpdateTxResult> updateFut = res.updateFuture();

                    if (updateFut != null) {
                        if (updateFut.isDone())
                            res = updateFut.get();
                        else {
                            CacheObject val0 = val;
                            GridDhtCacheEntry entry0 = entry;

                            it.beforeDetach();

                            updateFut.listen(new CI1<IgniteInternalFuture<GridCacheUpdateTxResult>>() {
                                @Override public void apply(IgniteInternalFuture<GridCacheUpdateTxResult> fut) {
                                    try {
                                        processEntry(entry0, op, fut.get(), val0);

                                        continueLoop(true);
                                    }
                                    catch (Throwable e) {
                                        onDone(e);
                                    }
                                }
                            });

                            // Can't move further. Exit loop without decrementing the counter.
                            return;
                        }
                    }

                    processEntry(entry, op, res, val);
                }

                if (!hasNext0()) {
                    if (walPtr != null && !cctx.tm().logTxRecords()) {
                        cctx.shared().wal().flush(walPtr, true);

                        walPtr = null; // Avoid additional flushing.
                    }

                    if (!F.isEmpty(batches)) {
                        // Flush incomplete batches.
                        // Need to skip batches for nodes where first request (contains tx info) is still in-flight.
                        // Otherwise, the regular enlist request (without tx info) may beat it to the primary node.
                        Iterator<Map.Entry<UUID, Batch>> it = batches.entrySet().iterator();

                        while (it.hasNext()) {
                            Map.Entry<UUID, Batch> e = it.next();

                            ConcurrentMap<Integer, Batch> pending0 =
                                pending == null ? null : pending.get(e.getKey());

                            if (pending0 == null || !pending0.containsKey(FIRST_BATCH_ID)) {
                                it.remove();

                                sendBatch(e.getValue());
                            }
                        }
                    }

                    if (noPendingRequests()) {
                        onDone(cnt);

                        return;
                    }
                }

                if (SKIP_UPD.decrementAndGet(this) == 0)
                    break;

                skipCntr = 1;
            }
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /** */
    private Object next0() {
        if (!hasNext0())
            throw new NoSuchElementException();

        Object cur;

        if ((cur = peek) != null)
            peek = null;
        else
            cur = it.next();

        return cur;
    }

    /** */
    private boolean hasNext0() {
        if (peek == null && !it.hasNext())
            peek = FINISHED;

        return peek != FINISHED;
    }

    /**
     * @return {@code True} if in-flight batches map is empty.
     */
    private boolean noPendingRequests() {
        if (F.isEmpty(pending))
            return true;

        for (ConcurrentMap<Integer, Batch> e : pending.values()) {
            if (!e.isEmpty())
                return false;
        }

        return true;
    }

    /**
     * @param entry Cache entry.
     * @param op Operation.
     * @param updRes Update result.
     * @param val New value.
     * @throws IgniteCheckedException If failed.
     */
    private void processEntry(GridDhtCacheEntry entry, EnlistOperation op,
        GridCacheUpdateTxResult updRes, CacheObject val) throws IgniteCheckedException {
        checkCompleted();

        assert updRes != null && updRes.updateFuture() == null;

        WALPointer ptr0 = updRes.loggedPointer();

        if (ptr0 != null)
            walPtr = ptr0;

        if (!updRes.success())
            return;

        cnt++;

        if (op != EnlistOperation.LOCK)
            addToBatch(entry.key(), val, updRes.mvccHistory(), updRes.updateCounter(), entry.context().cacheId());
    }

    /**
     * Adds row to batch.
     * <b>IMPORTANT:</b> This method should be called from the critical section in {@link this.sendNextBatches()}
     *
     * @param key Key.
     * @param val Value.
     * @param hist History rows.
     * @param updCntr Update counter.
     */
    private void addToBatch(KeyCacheObject key, CacheObject val, List<MvccLinkAwareSearchRow> hist, long updCntr,
        int cacheId) throws IgniteCheckedException {
        List<ClusterNode> backups = backupNodes(key);

        int part = cctx.affinity().partition(key);

        tx.touchPartition(cacheId, part);

        if (F.isEmpty(backups))
            return;

        CacheEntryInfoCollection hist0 = null;

        for (ClusterNode node : backups) {
            assert !node.isLocal();

            boolean moving = isMoving(node, part);

            if (skipNearNodeUpdates && node.id().equals(nearNodeId) && !moving) {
                updateMappings(node);

                if (newRemoteTx(node))
                    tx.addLockTransactionNode(node);

                hasNearNodeUpdates = true;

                if (nearUpdCntrs == null)
                    nearUpdCntrs = new GridLongList();

                nearUpdCntrs.add(updCntr);

                continue;
            }

            Batch batch = null;

            if (batches == null)
                batches = new HashMap<>();
            else
                batch = batches.get(node.id());

            if (batch == null)
                batches.put(node.id(), batch = new Batch(node));

            if (moving && hist0 == null) {
                assert !F.isEmpty(hist);

                hist0 = fetchHistoryInfo(key, hist);
            }

            batch.add(key, moving ? hist0 : val, updCntr);

            if (batch.size() == BATCH_SIZE) {
                assert batches != null;

                batches.remove(node.id());

                sendBatch(batch);
            }
        }
    }

    /**
     *
     * @param key Key.
     * @param hist History rows.
     * @return History entries.
     * @throws IgniteCheckedException, if failed.
     */
    private CacheEntryInfoCollection fetchHistoryInfo(KeyCacheObject key, List<MvccLinkAwareSearchRow> hist)
        throws IgniteCheckedException {
        List<GridCacheEntryInfo> res = new ArrayList<>();

        for (int i = 0; i < hist.size(); i++) {
            MvccLinkAwareSearchRow row0 = hist.get(i);

            MvccDataRow row = new MvccDataRow(cctx.group(),
                row0.hash(),
                row0.link(),
                key.partition(),
                CacheDataRowAdapter.RowData.NO_KEY,
                row0.mvccCoordinatorVersion(),
                row0.mvccCounter(),
                row0.mvccOperationCounter());

            GridCacheMvccEntryInfo entry = new GridCacheMvccEntryInfo();

            entry.version(row.version());
            entry.mvccVersion(row);
            entry.newMvccVersion(row);
            entry.value(row.value());
            entry.expireTime(row.expireTime());

            if (MvccUtils.compare(mvccSnapshot, row.mvccCoordinatorVersion(), row.mvccCounter()) != 0) {
                entry.mvccTxState(row.mvccTxState() != TxState.NA ? row.mvccTxState() :
                    MvccUtils.state(cctx, row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter()));
            }

            if (MvccUtils.compare(mvccSnapshot, row.newMvccCoordinatorVersion(), row.newMvccCounter()) != 0) {
                entry.newMvccTxState(row.newMvccTxState() != TxState.NA ? row.newMvccTxState() :
                    MvccUtils.state(cctx, row.newMvccCoordinatorVersion(), row.newMvccCounter(),
                    row.newMvccOperationCounter()));
            }

            res.add(entry);
        }

        return new CacheEntryInfoCollection(res);
    }

    /** */
    private boolean newRemoteTx(ClusterNode node) {
        Set<ClusterNode> nodes = tx.lockTransactionNodes();

        return nodes == null || !nodes.contains(node);
    }

    /**
     * Checks if there free space in batches or free slot in in-flight batches is available for the given key.
     *
     * @param key Key.
     * @return {@code True} if there is possible to add this key to batch or send ready batch.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private boolean ensureFreeSlot(KeyCacheObject key) {
        if (F.isEmpty(batches) || F.isEmpty(pending))
            return true;

        // Check possibility of adding to batch and sending.
        for (ClusterNode node : backupNodes(key)) {
            if (skipNearNodeUpdates && node.id().equals(nearNodeId) && !isMoving(node, key.partition()))
                continue;

            Batch batch = batches.get(node.id());

            // We can add key if batch is not full.
            if (batch == null || batch.size() < BATCH_SIZE - 1)
                continue;

            ConcurrentMap<Integer, Batch> pending0 = pending.get(node.id());

            assert pending0 == null || pending0.size() <= BATCHES_PER_NODE;

            if (pending0 != null && (pending0.containsKey(FIRST_BATCH_ID) || pending0.size() == BATCHES_PER_NODE))
                return false;
        }

        return true;
    }

    /**
     * Send batch request to remote data node.
     *
     * @param batch Batch.
     */
    private void sendBatch(Batch batch) throws IgniteCheckedException {
        assert batch != null && !batch.node().isLocal();

        ClusterNode node = batch.node();

        updateMappings(node);

        GridDhtTxQueryEnlistRequest req;

        if (newRemoteTx(node)) {
            tx.addLockTransactionNode(node);

            // If this is a first request to this node, send full info.
            req = new GridDhtTxQueryFirstEnlistRequest(cctx.cacheId(),
                futId,
                cctx.localNodeId(),
                tx.topologyVersionSnapshot(),
                lockVer,
                mvccSnapshot,
                tx.remainingTime(),
                tx.taskNameHash(),
                nearNodeId,
                nearLockVer,
                it.operation(),
                FIRST_BATCH_ID,
                batch.keys(),
                batch.values(),
                batch.updateCounters());
        }
        else {
            // Send only keys, values, LockVersion and batchId if this is not a first request to this backup.
            req = new GridDhtTxQueryEnlistRequest(cctx.cacheId(),
                futId,
                lockVer,
                it.operation(),
                ++batchIdCntr,
                mvccSnapshot.operationCounter(),
                batch.keys(),
                batch.values(),
                batch.updateCounters());
        }

        ConcurrentMap<Integer, Batch> pending0 = null;

        if (pending == null)
            pending = new ConcurrentHashMap<>();
        else
            pending0 = pending.get(node.id());

        if (pending0 == null)
            pending.put(node.id(), pending0 = new ConcurrentHashMap<>());

        Batch prev = pending0.put(req.batchId(), batch);

        assert prev == null;

        cctx.io().send(node, req, cctx.ioPolicy());
    }

    /** */
    private synchronized void updateMappings(ClusterNode node) throws IgniteCheckedException {
        checkCompleted();

        Map<UUID, GridDistributedTxMapping> m = tx.dhtMap;

        GridDistributedTxMapping mapping = m.get(node.id());

        if (mapping == null)
            m.put(node.id(), mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();
    }

    /**
     * @param key Key.
     * @return Backup nodes for the given key.
     */
    @NotNull private List<ClusterNode> backupNodes(KeyCacheObject key) {
        List<ClusterNode> dhtNodes = cctx.affinity().nodesByKey(key, tx.topologyVersion());

        assert !dhtNodes.isEmpty() && dhtNodes.get(0).id().equals(cctx.localNodeId()) :
            "localNode = " + cctx.localNodeId() + ", dhtNodes = " + dhtNodes;

        if (dhtNodes.size() == 1)
            return Collections.emptyList();

        return dhtNodes.subList(1, dhtNodes.size());
    }

    /**
     * Checks whether all the necessary partitions are in {@link GridDhtPartitionState#OWNING} state.
     *
     * @param parts Partitions.
     * @throws ClusterTopologyCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void checkPartitions(@Nullable int[] parts) throws ClusterTopologyCheckedException {
        if (cctx.isLocal() || !cctx.rebalanceEnabled())
            return;

        if (parts == null)
            parts = U.toIntArray(
                cctx.affinity()
                    .primaryPartitions(cctx.localNodeId(), tx.topologyVersionSnapshot()));

        GridDhtPartitionTopology top = cctx.topology();

        try {
            top.readLock();

            for (int i = 0; i < parts.length; i++) {
                GridDhtLocalPartition p = top.localPartition(parts[i]);

                if (p == null || p.state() != GridDhtPartitionState.OWNING)
                    throw new ClusterTopologyCheckedException("Cannot run update query. " +
                        "Node must own all the necessary partitions."); // TODO IGNITE-7185 Send retry instead.
            }
        }
        finally {
            top.readUnlock();
        }
    }

    /**
     * @param part Partition.
     * @return {@code true} if the given partition is rebalancing to any backup node.
     */
    private boolean isMoving(int part) {
        if (movingParts == null)
            movingParts = new HashMap<>();

        Boolean res = movingParts.get(part);

        if (res != null)
            return res;

        List<ClusterNode> dhtNodes = cctx.affinity().nodesByPartition(part, tx.topologyVersion());

        for (int i = 1; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);
            if (isMoving(node, part)) {
                movingParts.put(part, Boolean.TRUE);

                return true;
            }
        }

        movingParts.put(part, Boolean.FALSE);

        return false;
    }

    /**
     * @param node Cluster node.
     * @param part Partition.
     * @return {@code true} if the given partition is rebalancing to the given node.
     */
    private boolean isMoving(ClusterNode node, int part) {
        GridDhtPartitionState partState = cctx.topology().partitionState(node.id(), part);

        return partState != GridDhtPartitionState.OWNING && partState != GridDhtPartitionState.EVICTED;
    }

    /** */
    private void checkCompleted() throws IgniteCheckedException {
        if (isDone())
            throw new IgniteCheckedException("Future is done.");
    }

    /**
     * Callback on backup response.
     *
     * @param nodeId Backup node.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridDhtTxQueryEnlistResponse res) {
        if (res.error() != null) {
            onDone(new IgniteCheckedException("Failed to update backup node: [localNodeId=" + cctx.localNodeId() +
                ", remoteNodeId=" + nodeId + ']', res.error()));

            return;
        }

        assert pending != null;

        ConcurrentMap<Integer, Batch> pending0 = pending.get(nodeId);

        assert pending0 != null;

        Batch rmv = pending0.remove(res.batchId());

        assert rmv != null;

        continueLoop(false);
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean backupLeft = false;

        Set<ClusterNode> nodes = tx.lockTransactionNodes();

        if (!F.isEmpty(nodes)) {
            for (ClusterNode node : nodes) {
                if (node.id().equals(nodeId)) {
                    backupLeft = true;

                    break;
                }
            }
        }

        return (backupLeft || nearNodeId.equals(nodeId)) && onDone(
            new ClusterTopologyCheckedException((backupLeft ? "Backup" : "Requesting") +
                " node left the grid [nodeId=" + nodeId + ']'));
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
        assert res != null || err != null;

        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        if (err == null)
            clearLockFuture();

        // To prevent new remote transactions creation
        // after future is cancelled by rollback.
        synchronized (this) {
            boolean done = super.onDone(res, err);

            assert done;

            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeFuture(futId);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            U.close(it, log);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable error) {
        onDone(error);
    }

    /**
     * @return Timeout exception.
     */
    @NotNull protected IgniteTxTimeoutCheckedException timeoutException() {
        return new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
            "transaction [timeout=" + timeout + ", tx=" + tx + ']');
    }

    /**
     * A batch of rows
     */
    private static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** */
        private List<KeyCacheObject> keys;

        /**
         * Values collection.
         * Items can be either {@link CacheObject} or preload entries collection {@link CacheEntryInfoCollection}.
         */
        private List<Message> vals;

        /** Update counters. */
        private GridLongList updCntrs;

        /**
         * @param node Cluster node.
         */
        private Batch(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * Adds a row to batch.
         *
         * @param key Key.
         * @param val Value or preload entries collection.
         */
        public void add(KeyCacheObject key, Message val, long updCntr) {
            assert val == null || val instanceof CacheObject || val instanceof CacheEntryInfoCollection;
            assert updCntr > 0;

            if (keys == null)
                keys = new ArrayList<>();

            keys.add(key);

            if (val != null) {
                if (vals == null)
                    vals = new ArrayList<>();

                vals.add(val);
            }

            if (updCntrs == null)
                updCntrs = new GridLongList();

            updCntrs.add(updCntr);

            assert (vals == null) || keys.size() == vals.size();
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return keys == null ? 0 : keys.size();
        }

        /**
         * @return Collection of row keys.
         */
        public List<KeyCacheObject> keys() {
            return keys;
        }

        /**
         * @return Collection of row values.
         */
        public List<Message> values() {
            return vals;
        }

        /**
         * @return Update counters.
         */
        public GridLongList updateCounters() {
            return updCntrs;
        }
    }

    /**
     * Lock request timeout object.
     */
    protected class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onDone(timeoutException());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
