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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
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
 * Abstract future processing transaction enlisting and locking.
 */
public abstract class GridDhtTxAbstractEnlistFuture<T> extends GridCacheFutureAdapter<T>
    implements DhtLockFuture<T> {
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

    /** Transaction. */
    protected final GridDhtTxLocalAdapter tx;

    /** Lock version. */
    protected final GridCacheVersion lockVer;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** New DHT nodes. */
    protected Set<UUID> newDhtNodes = new HashSet<>();

    /** Near node ID. */
    protected final UUID nearNodeId;

    /** Near lock version. */
    protected final GridCacheVersion nearLockVer;

    /** Filter. */
    private final CacheEntryPredicate filter;

    /** Keep binary flag. */
    protected boolean keepBinary;

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

    /** Do not send DHT requests to near node. */
    protected boolean skipNearNodeUpdates;

    /** There are keys belonging to backup partitions on near node. */
    protected boolean hasNearNodeUpdates;

    /** Moving partitions. */
    private Map<Integer, Boolean> movingParts;

    /** Map for tracking nodes to which first request was already sent in order to send smaller subsequent requests. */
    private final Set<ClusterNode> firstReqSent = new HashSet<>();

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     * @param filter Filter.
     * @param keepBinary Keep binary flag.
     */
    protected GridDhtTxAbstractEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx,
        @Nullable CacheEntryPredicate filter,
        boolean keepBinary) {
        assert tx != null;
        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;

        this.threadId = threadId;
        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.timeout = timeout;
        this.tx = tx;
        this.filter = filter;
        this.keepBinary = keepBinary;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxAbstractEnlistFuture.class);
    }

    /**
     * Gets source to be updated iterator.
     *
     * @return iterator.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract UpdateSourceIterator<?> createIterator() throws IgniteCheckedException;

    /**
     * Gets query result.
     *
     * @return Query result.
     */
    protected abstract T result0();

    /**
     * Gets need previous value flag.
     *
     * @return {@code True} if previous value is required.
     */
    public boolean needResult() {
        return false;
    }

    /**
     * Entry processed callback.
     *
     * @param key Entry key.
     * @param res Update result.
     */
    protected abstract void onEntryProcessed(KeyCacheObject key, GridCacheUpdateTxResult res);

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

        while (true) {
            IgniteInternalFuture<?> fut = tx.lockFut;

            if (fut == GridDhtTxLocalAdapter.ROLLBACK_FUT) {
                onDone(tx.timedOut() ? tx.timeoutException() : tx.rollbackException());

                return;
            }
            else if (fut != null) {
                // Wait for previous future.
                assert fut instanceof GridNearTxAbstractEnlistFuture
                    || fut instanceof GridDhtTxAbstractEnlistFuture : fut;

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

        if (isDone()) {
            cctx.mvcc().removeFuture(futId);

            return;
        }

        assert added;

        if (timeoutObj != null)
            cctx.time().addTimeoutObject(timeoutObj);

        try {
            checkCoordinatorVersion();

            UpdateSourceIterator<?> it = createIterator();

            if (!it.hasNext()) {
                U.close(it, log);

                onDone(result0());

                return;
            }

            if (!tx.implicitSingle())
                tx.addActiveCache(cctx, false);
            else // Nothing to do for single update.
                assert tx.txState().cacheIds().contains(cctx.cacheId()) && tx.txState().cacheIds().size() == 1;

            tx.markQueryEnlisted();

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
                int curPart = -1;
                List<ClusterNode> backups = null;

                while (hasNext0()) {
                    Object cur = next0();

                    KeyCacheObject key = toKey(op, cur);

                    if (curPart != key.partition())
                        backups = backupNodes(curPart = key.partition());

                    assert backups != null;

                    if (!ensureFreeSlot(key, backups)) {
                        // Can't advance further at the moment.
                        peek = cur;

                        it.beforeDetach();

                        break;
                    }

                    GridDhtCacheEntry entry = cache.entryExx(key);

                    if (log.isDebugEnabled())
                        log.debug("Adding entry: " + entry);

                    assert !entry.detached();

                    CacheObject val = op.isDeleteOrLock() || op.isInvoke()
                        ? null : cctx.toCacheObject(((IgniteBiTuple)cur).getValue());

                    GridInvokeValue invokeVal = null;
                    EntryProcessor entryProc = null;
                    Object[] invokeArgs = null;

                    if (op.isInvoke()) {
                        assert needResult();

                        invokeVal = (GridInvokeValue)((IgniteBiTuple)cur).getValue();

                        entryProc = invokeVal.entryProcessor();
                        invokeArgs = invokeVal.invokeArgs();
                    }

                    assert entryProc != null || !op.isInvoke();

                    boolean needOldVal = tx.txState().useMvccCaching(cctx.cacheId());

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
                                        mvccSnapshot,
                                        isMoving(key.partition(), backups),
                                        needOldVal,
                                        filter,
                                        needResult());

                                    break;

                                case INSERT:
                                case TRANSFORM:
                                case UPSERT:
                                case UPDATE:
                                    res = entry.mvccSet(
                                        tx,
                                        cctx.localNodeId(),
                                        val,
                                        entryProc,
                                        invokeArgs,
                                        0,
                                        topVer,
                                        mvccSnapshot,
                                        op.cacheOperation(),
                                        isMoving(key.partition(), backups),
                                        op.noCreate(),
                                        needOldVal,
                                        filter,
                                        needResult(),
                                        keepBinary);

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

                    final Message val0 = invokeVal != null ? invokeVal : val;

                    if (updateFut != null) {
                        if (updateFut.isDone())
                            res = updateFut.get();
                        else {
                            GridDhtCacheEntry entry0 = entry;
                            List<ClusterNode> backups0 = backups;

                            it.beforeDetach();

                            updateFut.listen(new CI1<IgniteInternalFuture<GridCacheUpdateTxResult>>() {
                                @Override public void apply(IgniteInternalFuture<GridCacheUpdateTxResult> fut) {
                                    try {
                                        tx.incrementLockCounter();

                                        processEntry(entry0, op, fut.get(), val0, backups0);

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

                    tx.incrementLockCounter();

                    processEntry(entry, op, res, val0, backups);
                }

                if (!hasNext0()) {
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
                        onDone(result0());

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

    /** */
    private KeyCacheObject toKey(EnlistOperation op, Object cur) {
        KeyCacheObject key = cctx.toCacheKeyObject(op.isDeleteOrLock() ? cur : ((IgniteBiTuple)cur).getKey());

        if (key.partition() == -1)
            key.partition(cctx.affinity().partition(key));

        return key;
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
     * @param backups Backup nodes
     * @throws IgniteCheckedException If failed.
     */
    private void processEntry(GridDhtCacheEntry entry, EnlistOperation op,
        GridCacheUpdateTxResult updRes, Message val, List<ClusterNode> backups) throws IgniteCheckedException {
        checkCompleted();

        assert updRes != null && updRes.updateFuture() == null;

        if (op != EnlistOperation.LOCK)
            onEntryProcessed(entry.key(), updRes);

        if (!updRes.success()
            || updRes.filtered()
            || op == EnlistOperation.LOCK)
            return;

        cctx.shared().mvccCaching().addEnlisted(entry.key(), updRes.newValue(), 0, 0, lockVer,
            updRes.oldValue(), tx.local(), tx.topologyVersion(), mvccSnapshot, cctx.cacheId(), tx, null, -1);

        addToBatch(entry.key(), val, updRes.mvccHistory(), entry.context().cacheId(), backups);
    }

    /**
     * Adds row to batch.
     * <b>IMPORTANT:</b> This method should be called from the critical section in {@link this.sendNextBatches()}
     * @param key Key.
     * @param val Value.
     * @param hist History rows.
     * @param cacheId Cache Id.
     * @param backups Backup nodes
     */
    private void addToBatch(KeyCacheObject key, Message val, List<MvccLinkAwareSearchRow> hist,
        int cacheId, List<ClusterNode> backups) throws IgniteCheckedException {
        int part = key.partition();

        tx.touchPartition(cacheId, part);

        if (F.isEmpty(backups))
            return;

        CacheEntryInfoCollection hist0 = null;

        for (ClusterNode node : backups) {
            assert !node.isLocal();

            boolean moving = isMoving(node, part);

            if (skipNearLocalUpdate(node, moving)) {
                updateMappings(node);

                if (newRemoteTx(node))
                    addNewRemoteTxNode(node);

                hasNearNodeUpdates = true;

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
                assert !F.isEmpty(hist) || val == null;

                hist0 = fetchHistoryInfo(key, hist);
            }

            batch.add(key, moving ? hist0 : val);

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
    private CacheEntryInfoCollection fetchHistoryInfo(KeyCacheObject key, List<MvccLinkAwareSearchRow> hist) {
        List<GridCacheEntryInfo> res = new ArrayList<>();

        for (int i = 0; i < hist.size(); i++) {
            MvccLinkAwareSearchRow row0 = hist.get(i);

            MvccDataRow row = new MvccDataRow(cctx.group(),
                row0.hash(),
                row0.link(),
                key.partition(),
                CacheDataRowAdapter.RowData.NO_KEY_WITH_HINTS,
                row0.mvccCoordinatorVersion(),
                row0.mvccCounter(),
                row0.mvccOperationCounter(),
                false
            );

            GridCacheMvccEntryInfo entry = new GridCacheMvccEntryInfo();

            entry.cacheId(cctx.cacheId());
            entry.version(row.version());
            entry.value(row.value());
            entry.expireTime(row.expireTime());

            // Row should be retrieved with actual hints.
            entry.mvccVersion(row);
            entry.newMvccVersion(row);

            if (MvccUtils.compare(mvccSnapshot, row.mvccCoordinatorVersion(), row.mvccCounter()) != 0)
                entry.mvccTxState(row.mvccTxState());

            if (row.newMvccCoordinatorVersion() != MvccUtils.MVCC_CRD_COUNTER_NA
                && MvccUtils.compare(mvccSnapshot, row.newMvccCoordinatorVersion(), row.newMvccCounter()) != 0)
                entry.newMvccTxState(row.newMvccTxState());

            assert mvccSnapshot.coordinatorVersion() != MvccUtils.MVCC_CRD_COUNTER_NA;

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
     * Add new involved DHT node.
     *
     * @param node Node.
     */
    private void addNewRemoteTxNode(ClusterNode node) {
        tx.addLockTransactionNode(node);

        newDhtNodes.add(node.id());
    }

    /**
     * Checks if there free space in batches or free slot in in-flight batches is available for the given key.
     *
     * @param key Key.
     * @param backups Backup nodes.
     * @return {@code True} if there is possible to add this key to batch or send ready batch.
     */
    private boolean ensureFreeSlot(KeyCacheObject key, List<ClusterNode> backups) {
        if (F.isEmpty(batches) || F.isEmpty(pending))
            return true;

        int part = key.partition();

        // Check possibility of adding to batch and sending.
        for (ClusterNode node : backups) {
            if (skipNearLocalUpdate(node, isMoving(node, part)))
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

        if (newRemoteTx(node))
            addNewRemoteTxNode(node);

        if (firstReqSent.add(node)) {
            // If this is a first request to this node, send full info.
            req = new GridDhtTxQueryFirstEnlistRequest(cctx.cacheId(),
                futId,
                cctx.localNodeId(),
                tx.topologyVersionSnapshot(),
                lockVer,
                mvccSnapshot.withoutActiveTransactions(),
                tx.remainingTime(),
                tx.taskNameHash(),
                nearNodeId,
                nearLockVer,
                it.operation(),
                FIRST_BATCH_ID,
                batch.keys(),
                batch.values()
            );
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
                batch.values()
            );
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

        try {
            cctx.io().send(node, req, cctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException e) {
            // backup node left the grid, will continue.
            onNodeLeft(node.id());
        }
    }

    /** */
    private synchronized void updateMappings(ClusterNode node) throws IgniteCheckedException {
        checkCompleted();

        Map<UUID, GridDistributedTxMapping> m = tx.dhtMap;

        GridDistributedTxMapping mapping = m.get(node.id());

        if (mapping == null)
            m.put(node.id(), mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        checkCompleted();
    }

    /** */
    private boolean skipNearLocalUpdate(ClusterNode node, boolean moving) {
        return skipNearNodeUpdates && node.id().equals(nearNodeId) && !moving;
    }

    /**
     * @param part Partition.
     * @return Backup nodes for the given partition.
     */
    @NotNull private List<ClusterNode> backupNodes(int part) {
        List<ClusterNode> nodes = cctx.topology().nodes(part, tx.topologyVersion());

        assert !nodes.isEmpty() && nodes.get(0).isLocal();

        return nodes.subList(1, nodes.size());
    }

    /**
     * Checks whether new coordinator was initialized after the snapshot is acquired.
     *
     * Need to fit invariant that all updates are finished before a new coordinator is initialized.
     *
     * @throws ClusterTopologyCheckedException If failed.
     */
    private void checkCoordinatorVersion() throws ClusterTopologyCheckedException {
        MvccCoordinator crd = cctx.shared().coordinators().currentCoordinator();

        if (!crd.initialized() || crd.version() != mvccSnapshot.coordinatorVersion())
            throw new ClusterTopologyCheckedException("Cannot perform update, coordinator was changed: " +
                "[currentCoordinator=" + crd + ", mvccSnapshot=" + mvccSnapshot + "].");
    }

    /**
     * @param part Partition.
     * @param backups Backup nodes.
     * @return {@code true} if the given partition is rebalancing to any backup node.
     */
    private boolean isMoving(int part, List<ClusterNode> backups) {
        Boolean res;

        if (movingParts == null)
            movingParts = new HashMap<>();

        if ((res = movingParts.get(part)) == null)
            movingParts.put(part, res = isMoving0(part, backups));

        return res == Boolean.TRUE;
    }

    /**
     * @param part Partition.
     * @param backups Backup nodes.
     * @return {@code true} if the given partition is rebalancing to any backup node.
     */
    private Boolean isMoving0(int part, List<ClusterNode> backups) {
        for (ClusterNode node : backups) {
            if (isMoving(node, part))
                return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * @param node Cluster node.
     * @param part Partition.
     * @return {@code true} if the given partition is rebalancing to the given node.
     */
    private boolean isMoving(ClusterNode node, int part) {
        return cctx.topology().partitionState(node.id(), part) == GridDhtPartitionState.MOVING;
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
        try {
            if (nearNodeId.equals(nodeId))
                onDone(new ClusterTopologyCheckedException("Requesting node left the grid [nodeId=" + nodeId + ']'));
            else if (pending != null && pending.remove(nodeId) != null)
                cctx.kernalContext().closure().runLocalSafe(() -> continueLoop(false));
        }
        catch (Exception e) {
            onDone(e);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err) {
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
        public void add(KeyCacheObject key, Message val) {
            assert val == null || val instanceof GridInvokeValue || val instanceof CacheObject
                || val instanceof CacheEntryInfoCollection;

            if (keys == null)
                keys = new ArrayList<>();

            if (vals == null && val != null) {
                vals = new ArrayList<>(U.ceilPow2(keys.size() + 1));

                while (vals.size() != keys.size())
                    vals.add(null); // Init vals with missed 'nulls'.
            }

            keys.add(key);

            if (vals != null)
                vals.add(val);
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
