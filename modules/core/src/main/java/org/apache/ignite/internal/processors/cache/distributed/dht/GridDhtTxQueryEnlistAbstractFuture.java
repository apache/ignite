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
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
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
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Abstract future processing transaction enlisting and locking
 * of entries produced with DML and SELECT FOR UPDATE queries.
 */
public abstract class GridDhtTxQueryEnlistAbstractFuture<T extends ExceptionAware> extends GridCacheFutureAdapter<T>
    implements DhtLockFuture<T> {
    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxQueryEnlistAbstractFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxQueryEnlistAbstractFuture.class, "done");

    /** SkipCntr field updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxQueryEnlistAbstractFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxQueryEnlistAbstractFuture.class, "skipCntr");

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

    /** Topology version. */
    protected final AffinityTopologyVersion topVer;

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

    /** Batches containing keys for moving partitions. */
    private Map<UUID, Batch> histBatches;

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

    /**
     *
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param parts Partitions.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    protected GridDhtTxQueryEnlistAbstractFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
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
        assert topVer != null && topVer.topologyVersion() > 0;
        assert threadId == tx.threadId();

        this.threadId = threadId;
        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.topVer = topVer;
        this.timeout = timeout;
        this.tx = tx;
        this.parts = parts;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxQueryEnlistAbstractFuture.class);
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
                    || fut instanceof GridDhtTxQueryEnlistAbstractFuture
                    || fut instanceof CompoundLockFuture
                    || fut instanceof GridNearTxSelectForUpdateFuture : fut;

                // Terminate this future if parent future is terminated by rollback.
                fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        if (fut.error() != null)
                            onDone(fut.error());
                    }
                });

                break;
            }
            else if (updateLockFuture())
                break;
        }

        boolean added = cctx.mvcc().addFuture(this, futId);

        assert added;

        if (timeoutObj != null)
            cctx.time().addTimeoutObject(timeoutObj);

        try {
            checkPartitions(parts);

            UpdateSourceIterator<?> it = createIterator();

            if (!it.hasNext()) {
                T res = createResponse();

                U.close(it, log);

                onDone(res);

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
     * @return {@code True} if future was updated successfully.
     */
    protected boolean updateLockFuture() {
        return tx.updateLockFuture(null, this);
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
        GridCacheOperation op = it.operation();

        try {
            while (true) {
                while (hasNext0()) {
                    Object cur = next0();

                    KeyCacheObject key = cctx.toCacheKeyObject(op == DELETE || op == READ ? cur : ((IgniteBiTuple)cur).getKey());

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

                    CacheObject val = null;

                    if (op == CREATE || op == UPDATE)
                        val = cctx.toCacheObject(((IgniteBiTuple)cur).getValue());

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
                                        isMovingPart(key.partition()));

                                    break;

                                case CREATE:
                                case UPDATE:
                                    res = entry.mvccSet(
                                        tx,
                                        cctx.localNodeId(),
                                        val,
                                        0,
                                        topVer,
                                        null,
                                        mvccSnapshot,
                                        op,
                                        isMovingPart(key.partition()));

                                    break;

                                case READ:
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

                    flushBatches();

                    if (noPendingRequests()) {
                        onDone(createResponse());

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
        else {
            cur = it.next();

            if (!it.hasNext())
                peek = FINISHED;
        }

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
    private void processEntry(GridDhtCacheEntry entry, GridCacheOperation op,
        GridCacheUpdateTxResult updRes, CacheObject val) throws IgniteCheckedException {
        checkCompleted();

        assert updRes != null && updRes.updateFuture() == null;

        WALPointer ptr0 = updRes.loggedPointer();

        if (ptr0 != null)
            walPtr = ptr0;

        if (!tx.queryEnlisted())
            tx.queryEnlisted(true);

        cnt++;

        if (op != READ)
            addToBatch(entry.key(), val, updRes.mvccHistory());
    }

    /**
     * Adds row to batch.
     * <b>IMPORTANT:</b> This method should be called from the critical section in {@link this.sendNextBatches()}
     *
     * @param key Key.
     * @param val Value.
     * @param hist History rows.
     */
    private void addToBatch(KeyCacheObject key, CacheObject val, List<MvccLinkAwareSearchRow> hist)
        throws IgniteCheckedException {
        List<ClusterNode> backups = backupNodes(key);

        if (F.isEmpty(backups))
            return;

        List<GridCacheEntryInfo> hist0 = null;

        int part = cctx.affinity().partition(key);

        for (ClusterNode node : backups) {
            assert !node.isLocal();

            GridDhtPartitionState partState = cctx.topology().partitionState(node.id(), part);

            boolean movingPart =
                partState != GridDhtPartitionState.OWNING && partState != GridDhtPartitionState.EVICTED;

            if (skipNearNodeUpdates && node.id().equals(nearNodeId) && !movingPart) {
                updateMappings(node);

                if (newRemoteTx(node))
                    tx.addLockTransactionNode(node);

                hasNearNodeUpdates = true;

                continue;
            }

            if (movingPart && hist0 == null) {
                assert !F.isEmpty(hist);

                hist0 = fetchHistoryInfo(key, hist);
            }

            Batch batch = getBatch(node, movingPart);

            batch.add(key, val, hist0);

            if (batch.size() == BATCH_SIZE && !firstBatchIsPending(node.id())) {
                removeBatch(node.id(), movingPart);

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
    private List<GridCacheEntryInfo> fetchHistoryInfo(KeyCacheObject key, List<MvccLinkAwareSearchRow> hist)
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

        return res;
    }

    /**
     * @param node Node.
     * @param movingPart Flag to indicate moving partition.
     * @return Batch.
     */
    private Batch getBatch(ClusterNode node, boolean movingPart) {
        Map<UUID, Batch> map = movingPart ? histBatches : batches;

        if (map == null) {
            if (movingPart)
                map = histBatches = new HashMap<>();
            else
                map = batches = new HashMap<>();
        }

        Batch batch = map.get(node.id());

        if (batch == null) {
            batch = new Batch(node);

            map.put(node.id(), batch);
        }

        return batch;
    }

    /**
     * @param nodeId Node id.
     * @param movingPart Flag to indicate moving partition.
     */
    private void removeBatch(UUID nodeId, boolean movingPart) {
        Map<UUID, Batch> map = movingPart ? histBatches : batches;

        assert map != null;

        map.remove(nodeId);
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
        if ((F.isEmpty(batches) && F.isEmpty(histBatches)) || F.isEmpty(pending))
            return true;

        // Check possibility of adding to batch and sending.
        for (ClusterNode node : backupNodes(key)) {
            GridDhtPartitionState partState = cctx.topology().partitionState(node.id(), key.partition());

            boolean movingPart =
                (partState != GridDhtPartitionState.OWNING && partState != GridDhtPartitionState.EVICTED);

            if (skipNearNodeUpdates && node.id().equals(nearNodeId) && !movingPart)
                continue;

            Batch batch = getBatch(node, movingPart);

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
     *
     * @param id Node id.
     * @return {@code true} if first batch is pending.
     */
    public boolean firstBatchIsPending(UUID id) {
        if (pending == null)
            return false;

        ConcurrentMap<Integer, Batch> pending0 = pending.get(id);

        return pending0 != null && pending0.containsKey(FIRST_BATCH_ID);
    }

    /**
     * Send batch request to remote data node.
     *
     * @param batch Batch.
     */
    private synchronized void sendBatch(Batch batch) throws IgniteCheckedException {
        checkCompleted();

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
                topVer,
                lockVer,
                mvccSnapshot,
                tx.remainingTime(),
                tx.taskNameHash(),
                nearNodeId,
                nearLockVer,
                it.operation(),
                FIRST_BATCH_ID,
                batch.keys(),
                batch.values());
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
                batch.values());
        }

        req.entries(batch.entries());

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
    private void updateMappings(ClusterNode node) {
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
                    .primaryPartitions(cctx.localNodeId(), topVer));

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
     * @param batchesMap Batches map to flush.
     * @return Map containing remaining batches.
     * @throws IgniteCheckedException If failed.
     */
    private Map<UUID, Batch> flushBatches(Map<UUID, Batch> batchesMap) throws IgniteCheckedException {
        Map<UUID, Batch> remains = null;

        if (!F.isEmpty(batchesMap)) {
            // Flush incomplete batches.
            for (Batch batch : batchesMap.values()) {
                UUID id = batch.node().id();
                if (firstBatchIsPending(id)) {
                    if (remains == null)
                        remains = new HashMap<>();

                    remains.put(id, batch);
                }
                else
                    sendBatch(batch);
            }
        }

        return remains;
    }

    /** */
    private void flushBatches() throws IgniteCheckedException {
        batches = flushBatches(batches);
        histBatches = flushBatches(histBatches);
    }

    /**
     * @param part Partition.
     * @return {@code true} if the given partition is rebalancing to any backup node.
     */
    private boolean isMovingPart(int part) {
        if (movingParts == null)
            movingParts = new HashMap<>();

        Boolean res = movingParts.get(part);

        if (res != null)
            return res;

        List<ClusterNode> dhtNodes = cctx.affinity().nodesByPartition(part, tx.topologyVersion());

        for (int i = 1; i < dhtNodes.size(); i++) {
            GridDhtPartitionState partState = cctx.topology().partitionState(dhtNodes.get(i).id(), part);

            if (partState != GridDhtPartitionState.OWNING && partState != GridDhtPartitionState.EVICTED) {
                movingParts.put(part, Boolean.TRUE);

                return true;
            }
        }

        movingParts.put(part, Boolean.FALSE);

        return false;
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
            new ClusterTopologyCheckedException(backupLeft ? "Backup" : "Requesting" +
                " node left the grid [nodeId=" + nodeId + ']'));
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err) {
        assert res != null ^ err != null;

        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        if (err != null)
            res = createResponse(err);

        assert res != null;

        if (res.error() == null)
            clearLockFuture();

        // To prevent new remote transactions creation
        // after future is cancelled by rollback.
        synchronized (this) {
            boolean done = super.onDone(res, null);

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
     * @param err Error.
     * @return Prepared response.
     */
    public abstract T createResponse(@NotNull Throwable err);

    /**
     * @return Prepared response.
     */
    public abstract T createResponse();

    /**
     * A batch of rows
     */
    private static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** */
        private List<KeyCacheObject> keys;

        /** */
        private List<CacheObject> vals;

        /** */
        private List<CacheEntryInfoCollection> entries;

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
         * @param val Value.
         * @param entryInfos History entries.
         */
        public void add(KeyCacheObject key, CacheObject val, List<GridCacheEntryInfo> entryInfos) {
            if (keys == null)
                keys = new ArrayList<>();

            keys.add(key);

            if (val != null) {
                if (vals == null)
                    vals = new ArrayList<>();

                vals.add(val);
            }

            if (entryInfos != null) {
                if (entries == null)
                    entries = new ArrayList<>();

                entries.add(new CacheEntryInfoCollection(entryInfos));
            }

            assert (vals == null) || keys.size() == vals.size();
            assert (entries == null) || keys.size() == entries.size();
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return keys == null ? 0 : keys.size();
        }

        /**
         * @return Collection of rows.
         */
        public List<KeyCacheObject> keys() {
            return keys;
        }

        /**
         * @return Collection of rows.
         */
        public List<CacheObject> values() {
            return vals;
        }

        /**
         * @return Historical entries.
         */
        public List<CacheEntryInfoCollection> entries() {
            return entries;
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
