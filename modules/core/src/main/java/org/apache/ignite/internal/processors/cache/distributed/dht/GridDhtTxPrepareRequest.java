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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * DHT prepare request.
 */
public class GridDhtTxPrepareRequest extends GridDistributedTxPrepareRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Max order. */
    private UUID nearNodeId;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private int miniId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Invalidate near entries flags. */
    private BitSet invalidateNearEntries;

    /** Near writes. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxEntry.class)
    private Collection<IgniteTxEntry> nearWrites;

    /** Owned versions by key. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey, GridCacheVersion> owned;

    /** Owned keys. */
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> ownedKeys;

    /** Owned values. */
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> ownedVals;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Preload keys. */
    private BitSet preloadKeys;

    /** */
    @GridDirectTransient
    private List<IgniteTxKey> nearWritesCacheMissed;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** {@code True} if remote tx should skip adding itself to completed versions map on finish. */
    private boolean skipCompletedVers;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param timeout Transaction timeout.
     * @param dhtWrites DHT writes.
     * @param nearWrites Near writes.
     * @param txNodes Transaction nodes mapping.
     * @param nearXidVer Near transaction ID.
     * @param last {@code True} if this is last prepare request for node.
     * @param addDepInfo Deployment info flag.
     * @param storeWriteThrough Cache store write through flag.
     * @param retVal Need return value flag.
     */
    public GridDhtTxPrepareRequest(
        IgniteUuid futId,
        int miniId,
        AffinityTopologyVersion topVer,
        GridDhtTxLocalAdapter tx,
        long timeout,
        Collection<IgniteTxEntry> dhtWrites,
        Collection<IgniteTxEntry> nearWrites,
        Map<UUID, Collection<UUID>> txNodes,
        GridCacheVersion nearXidVer,
        boolean last,
        boolean onePhaseCommit,
        UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean storeWriteThrough,
        boolean retVal,
        MvccSnapshot mvccInfo) {
        super(tx,
            timeout,
            null,
            dhtWrites,
            txNodes,
            retVal,
            last,
            onePhaseCommit,
            addDepInfo);

        assert futId != null;
        assert miniId != 0;

        this.topVer = topVer;
        this.futId = futId;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.nearXidVer = nearXidVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.mvccSnapshot = mvccInfo;

        storeWriteThrough(storeWriteThrough);
        needReturnValue(retVal);

        invalidateNearEntries = new BitSet(dhtWrites == null ? 0 : dhtWrites.size());

        nearNodeId = tx.nearNodeId();

        skipCompletedVers = tx.xidVersion() == tx.nearXidVersion();
    }

    /**
     * @return Mvcc info.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Near cache writes for which cache was not found (possible if client near cache was closed).
     */
    @Nullable public List<IgniteTxKey> nearWritesCacheMissed() {
        return nearWritesCacheMissed;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Near writes.
     */
    public Collection<IgniteTxEntry> nearWrites() {
        return nearWrites == null ? Collections.<IgniteTxEntry>emptyList() : nearWrites;
    }

    /**
     * @param idx Entry index to set invalidation flag.
     * @param invalidate Invalidation flag value.
     */
    void invalidateNearEntry(int idx, boolean invalidate) {
        invalidateNearEntries.set(idx, invalidate);
    }

    /**
     * @param idx Index to get invalidation flag value.
     * @return Invalidation flag value.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateNearEntries.get(idx);
    }

    /**
     * Marks last added key for preloading.
     *
     * @param idx Key index.
     */
    void markKeyForPreload(int idx) {
        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx, true);
    }

    /**
     * Checks whether entry info should be sent to primary node from backup.
     *
     * @param idx Index.
     * @return {@code True} if value should be sent, {@code false} otherwise.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(IgniteTxKey key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @return Owned versions map.
     */
    public Map<IgniteTxKey, GridCacheVersion> owned() {
        return owned;
    }

    /**
     * @return {@code True} if remote tx should skip adding itself to completed versions map on finish.
     */
    public boolean skipCompletedVersion() {
        return skipCompletedVers;
    }

    /**
     * {@inheritDoc}
     *
     * @param ctx
     */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (owned != null && ownedKeys == null) {
            ownedKeys = owned.keySet();

            ownedVals = owned.values();

            for (IgniteTxKey key: ownedKeys) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);

                if (addDepInfo)
                    prepareObject(key, cctx);
            }
        }

        if (nearWrites != null)
            marshalTx(nearWrites, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedKeys != null) {
            assert ownedKeys.size() == ownedVals.size();

            owned = U.newHashMap(ownedKeys.size());

            Iterator<IgniteTxKey> keyIter = ownedKeys.iterator();

            Iterator<GridCacheVersion> valIter = ownedVals.iterator();

            while (keyIter.hasNext()) {
                IgniteTxKey key = keyIter.next();

                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(key.cacheId());

                if (cacheCtx != null) {
                    key.finishUnmarshal(cacheCtx, ldr);

                    owned.put(key, valIter.next());
                }
            }
        }

        if (nearWrites != null) {
            for (Iterator<IgniteTxEntry> it = nearWrites.iterator(); it.hasNext();) {
                IgniteTxEntry e = it.next();

                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(e.cacheId());

                if (cacheCtx == null) {
                    it.remove();

                    if (nearWritesCacheMissed == null)
                        nearWritesCacheMissed = new ArrayList<>();

                    nearWritesCacheMissed.add(e.txKey());
                }
                else {
                    e.context(cacheCtx);

                    e.unmarshal(ctx, true, ldr);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 20:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeBitSet("invalidateNearEntries", invalidateNearEntries))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("nearWrites", nearWrites, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeCollection("ownedKeys", ownedKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeCollection("ownedVals", ownedVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeBitSet("preloadKeys", preloadKeys))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeBoolean("skipCompletedVers", skipCompletedVers))
                    return false;

                writer.incrementState();

            case 30:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 31:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 32:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 33:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 20:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                invalidateNearEntries = reader.readBitSet("invalidateNearEntries");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                nearWrites = reader.readCollection("nearWrites", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                ownedKeys = reader.readCollection("ownedKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                ownedVals = reader.readCollection("ownedVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                preloadKeys = reader.readBitSet("preloadKeys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                skipCompletedVers = reader.readBoolean("skipCompletedVers");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 30:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 31:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 32:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 33:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxPrepareRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 34;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 34;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareRequest.class, this, "super", super.toString());
    }
}
