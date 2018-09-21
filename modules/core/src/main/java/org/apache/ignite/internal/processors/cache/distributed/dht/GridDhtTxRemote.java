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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteSingleStateImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteStateImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;

/**
 * Transaction created by system implicitly on remote nodes.
 */
public class GridDhtTxRemote extends GridDistributedTxRemoteAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Remote future ID. */
    private IgniteUuid rmtFutId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Store write through flag. */
    private boolean storeWriteThrough;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxRemote() {
        // No-op.
    }

    /**
     * This constructor is meant for optimistic transactions.
     *
     * @param ctx Cache context.
     * @param nearNodeId Near node ID.
     * @param rmtFutId Remote future ID.
     * @param nodeId Node ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param nearXidVer Near transaction ID.
     * @param txNodes Transaction nodes mapping.
     * @param storeWriteThrough Cache store write through flag.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext ctx,
        UUID nearNodeId,
        IgniteUuid rmtFutId,
        UUID nodeId,
        AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        GridCacheVersion nearXidVer,
        Map<UUID, Collection<UUID>> txNodes,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean single,
        boolean storeWriteThrough) {
        super(
            ctx,
            nodeId,
            xidVer,
            commitVer,
            sys,
            plc,
            concurrency,
            isolation,
            invalidate,
            timeout,
            txSize,
            subjId,
            taskNameHash
        );

        assert nearNodeId != null;
        assert rmtFutId != null;

        this.nearNodeId = nearNodeId;
        this.rmtFutId = rmtFutId;
        this.nearXidVer = nearXidVer;
        this.txNodes = txNodes;
        this.storeWriteThrough = storeWriteThrough;

        txState = single ? new IgniteTxRemoteSingleStateImpl() :
            new IgniteTxRemoteStateImpl(
            Collections.<IgniteTxKey, IgniteTxEntry>emptyMap(),
            new ConcurrentLinkedHashMap<IgniteTxKey, IgniteTxEntry>(U.capacity(txSize), 0.75f, 1));

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /**
     * This constructor is meant for pessimistic transactions.
     *
     * @param ctx Cache context.
     * @param nearNodeId Near node ID.
     * @param rmtFutId Remote future ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param topVer Topology version.
     * @param xidVer XID version.
     * @param commitVer Commit version.
     * @param sys System flag.
     * @param concurrency Concurrency level (should be pessimistic).
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param timeout Timeout.
     * @param txSize Expected transaction size.
     * @param storeWriteThrough Cache store write through flag.
     */
    public GridDhtTxRemote(
        GridCacheSharedContext ctx,
        UUID nearNodeId,
        IgniteUuid rmtFutId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        AffinityTopologyVersion topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean invalidate,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean storeWriteThrough) {
        super(
            ctx,
            nodeId,
            xidVer,
            commitVer,
            sys,
            plc,
            concurrency,
            isolation,
            invalidate,
            timeout,
            txSize,
            subjId,
            taskNameHash
        );

        assert nearNodeId != null;
        assert rmtFutId != null;

        this.nearXidVer = nearXidVer;
        this.nearNodeId = nearNodeId;
        this.rmtFutId = rmtFutId;
        this.storeWriteThrough = storeWriteThrough;

        txState = new IgniteTxRemoteStateImpl(
            Collections.<IgniteTxKey, IgniteTxEntry>emptyMap(),
            new ConcurrentLinkedHashMap<IgniteTxKey, IgniteTxEntry>(U.capacity(txSize), 0.75f, 1));

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /**
     * @param txNodes Transaction nodes.
     */
    @Override public void transactionNodes(Map<UUID, Collection<UUID>> txNodes) {
        this.txNodes = txNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean remote() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean dht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override  public boolean storeWriteThrough() {
        return storeWriteThrough;
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId();
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        Collection<UUID> res = new ArrayList<>(2);

        res.add(nearNodeId);
        res.add(nodeId);

        return res;
    }

    /** {@inheritDoc} */
    @Override public UUID otherNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Remote future ID.
     */
    IgniteUuid remoteFutureId() {
        return rmtFutId;
    }

    /** {@inheritDoc} */
    @Override protected boolean updateNearCache(GridCacheContext cacheCtx, KeyCacheObject key, AffinityTopologyVersion topVer) {
        if (!cacheCtx.isDht() || !isNearEnabled(cacheCtx) || cctx.localNodeId().equals(nearNodeId))
            return false;

        if (cacheCtx.config().getBackups() == 0)
            return true;

        // Check if we are on the backup node.
        return !cacheCtx.affinity().backupsByKey(key, topVer).contains(cctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext cacheCtx, int part) {
        super.addInvalidPartition(cacheCtx, part);

        txState.invalidPartition(part);
    }

    /**
     * @param entry Write entry.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void addWrite(IgniteTxEntry entry, ClassLoader ldr) throws IgniteCheckedException {
        entry.unmarshal(cctx, false, ldr);

        GridCacheContext cacheCtx = entry.context();

        try {
            GridDhtCacheEntry cached = cacheCtx.dht().entryExx(entry.key(), topologyVersion());

            checkInternal(entry.txKey());

            // Initialize cache entry.
            entry.cached(cached);

            txState.addWriteEntry(entry.txKey(), entry);

            addExplicit(entry);
        }
        catch (GridDhtInvalidPartitionException e) {
            addInvalidPartition(cacheCtx, e.partition());
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param op Write operation.
     * @param key Key to add to write set.
     * @param val Value.
     * @param entryProcessors Entry processors.
     * @param ttl TTL.
     * @param skipStore Skip store flag.
     */
    public void addWrite(GridCacheContext cacheCtx,
        GridCacheOperation op,
        IgniteTxKey key,
        @Nullable CacheObject val,
        @Nullable Collection<T2<EntryProcessor<Object, Object, Object>, Object[]>> entryProcessors,
        long ttl,
        boolean skipStore,
        boolean keepBinary) {
        checkInternal(key);

        if (isSystemInvalidate())
            return;

        GridDhtCacheEntry cached = cacheCtx.dht().entryExx(key.key(), topologyVersion());

        IgniteTxEntry txEntry = new IgniteTxEntry(cacheCtx,
            this,
            op,
            val,
            ttl,
            -1L,
            cached,
            null,
            skipStore,
            keepBinary);

        txEntry.entryProcessors(entryProcessors);

        txState.addWriteEntry(key, txEntry);
    }

    /**
     *
     * @param ctx Cache context.
     * @param op Operation.
     * @param keys Keys.
     * @param vals Values.
     * @param snapshot Mvcc snapshot.
     * @param updCntrs Update counters.
     * @throws IgniteCheckedException If failed.
     */
    public void mvccEnlistBatch(GridCacheContext ctx, EnlistOperation op, List<KeyCacheObject> keys,
        List<Message> vals, MvccSnapshot snapshot, GridLongList updCntrs) throws IgniteCheckedException {
        assert keys != null && updCntrs != null && keys.size() == updCntrs.size();

        WALPointer ptr = null;

        GridDhtCacheAdapter dht = ctx.dht();

        addActiveCache(ctx, false);

        for (int i = 0; i < keys.size(); i++) {
            KeyCacheObject key = keys.get(i);

            assert key != null;

            int part = ctx.affinity().partition(key);

            GridDhtLocalPartition locPart = ctx.topology().localPartition(part, topologyVersion(), false);

            if (locPart == null || !locPart.reserve())
                throw new ClusterTopologyException("Can not reserve partition. Please retry on stable topology.");

            try {
                CacheObject val = null;

                Message val0 = vals != null ? vals.get(i) : null;

                CacheEntryInfoCollection entries =
                    val0 instanceof CacheEntryInfoCollection ? (CacheEntryInfoCollection)val0 : null;

                if (entries == null && !op.isDeleteOrLock())
                    val = (val0 instanceof CacheObject) ? (CacheObject)val0 : null;

                GridDhtCacheEntry entry = dht.entryExx(key, topologyVersion());

                GridCacheUpdateTxResult updRes;

                while (true) {
                    ctx.shared().database().checkpointReadLock();

                    try {
                        if (entries == null) {
                            switch (op) {
                                case DELETE:
                                    updRes = entry.mvccRemove(
                                        this,
                                        ctx.localNodeId(),
                                        topologyVersion(),
                                        updCntrs.get(i),
                                        snapshot,
                                        false);

                                    break;

                                case INSERT:
                                case UPSERT:
                                case UPDATE:
                                    updRes = entry.mvccSet(
                                        this,
                                        ctx.localNodeId(),
                                        val,
                                        0,
                                        topologyVersion(),
                                        updCntrs.get(i),
                                        snapshot,
                                        op.cacheOperation(),
                                        false,
                                        false);

                                    break;

                                default:
                                    throw new IgniteSQLException("Cannot acquire lock for operation [op= "
                                        + op + "]" + "Operation is unsupported at the moment ",
                                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                            }
                        }
                        else {
                            updRes = entry.mvccUpdateRowsWithPreloadInfo(this,
                                ctx.localNodeId(),
                                topologyVersion(),
                                updCntrs.get(i),
                                entries.infos(),
                                op.cacheOperation(),
                                snapshot);
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        entry = dht.entryExx(key);
                    }
                    finally {
                        ctx.shared().database().checkpointReadUnlock();
                    }
                }

                assert updRes.updateFuture() == null : "Entry should not be locked on the backup";

                ptr = updRes.loggedPointer();
            }
            finally {
                locPart.release();
            }
        }

        if (ptr != null && !ctx.tm().logTxRecords())
            ctx.shared().wal().flush(ptr, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxRemote.class, this, "super", super.toString());
    }
}
