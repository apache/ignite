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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DHT lock request.
 */
public class GridDhtLockRequest extends GridDistributedLockRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near keys to lock. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> nearKeys;

    /** Invalidate reader flags. */
    private BitSet invalidateEntries;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Owner mapped version, if any. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<KeyCacheObject, GridCacheVersion> owned;

    /** Array of keys from {@link #owned}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    private KeyCacheObject[] ownedKeys;

    /** Array of values from {@link #owned}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    private GridCacheVersion[] ownedValues;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Indexes of keys needed to be preloaded. */
    private BitSet preloadKeys;

    /** TTL for read operation. */
    private long accessTtl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtLockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param lockVer Cache version.
     * @param topVer Topology version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param dhtCnt DHT count.
     * @param nearCnt Near count.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param accessTtl TTL for read operation.
     * @param skipStore Skip store flag.
     */
    public GridDhtLockRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion nearXidVer,
        long threadId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion lockVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean isInTx,
        boolean isRead,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int dhtCnt,
        int nearCnt,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        long accessTtl,
        boolean skipStore
    ) {
        super(cacheId,
            nodeId,
            nearXidVer,
            threadId,
            futId,
            lockVer,
            isInTx,
            isRead,
            isolation,
            isInvalidate,
            timeout,
            dhtCnt == 0 ? nearCnt : dhtCnt,
            txSize,
            skipStore);

        this.topVer = topVer;

        nearKeys = nearCnt == 0 ? Collections.<KeyCacheObject>emptyList() : new ArrayList<KeyCacheObject>(nearCnt);
        invalidateEntries = new BitSet(dhtCnt == 0 ? nearCnt : dhtCnt);

        assert miniId != null;

        this.miniId = miniId;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nodeId();
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Adds a Near key.
     *
     * @param key Key.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addNearKey(KeyCacheObject key, GridCacheSharedContext ctx)
        throws IgniteCheckedException {
        nearKeys.add(key);
    }

    /**
     * @return Near keys.
     */
    public List<KeyCacheObject> nearKeys() {
        return nearKeys == null ? Collections.<KeyCacheObject>emptyList() : nearKeys;
    }

    /**
     * Adds a DHT key.
     *
     * @param key Key.
     * @param invalidateEntry Flag indicating whether node should attempt to invalidate reader.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addDhtKey(
        KeyCacheObject key,
        boolean invalidateEntry,
        GridCacheContext ctx
    ) throws IgniteCheckedException {
        invalidateEntries.set(idx, invalidateEntry);

        addKeyBytes(key, false, null, ctx);
    }

    /**
     * Marks last added key for preloading.
     */
    public void markLastKeyForPreload() {
        assert idx > 0;

        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx - 1, true);
    }

    /**
     * @param idx Key index.
     * @return {@code True} if need to preload key with given index.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(KeyCacheObject key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @param key Key.
     * @return Owner and its mapped versions.
     */
    @Nullable public GridCacheVersion owned(KeyCacheObject key) {
        return owned == null ? null : owned.get(key);
    }

    /**
     * @param idx Entry index to check.
     * @return {@code True} if near entry should be invalidated.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateEntries.get(idx);
    }

    /**
     * @return Mini ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return TTL for read operation.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId));

        if (owned != null) {
            ownedKeys = new KeyCacheObject[owned.size()];
            ownedValues = new GridCacheVersion[ownedKeys.length];

            int i = 0;

            for (Map.Entry<KeyCacheObject, GridCacheVersion> entry : owned.entrySet()) {
                ownedKeys[i] = entry.getKey();
                ownedValues[i] = entry.getValue();
                i++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId), ldr);

        if (ownedKeys != null) {
            owned = new GridLeanMap<>(ownedKeys.length);

            for (int i = 0; i < ownedKeys.length; i++) {
                ownedKeys[i].finishUnmarshal(ctx.cacheContext(cacheId).cacheObjectContext(), ldr);
                owned.put(ownedKeys[i], ownedValues[i]);
            }

            ownedKeys = null;
            ownedValues = null;
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
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeBitSet("invalidateEntries", invalidateEntries))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection("nearKeys", nearKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeObjectArray("ownedKeys", ownedKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeObjectArray("ownedValues", ownedValues, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeBitSet("preloadKeys", preloadKeys))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeMessage("topVer", topVer))
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
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                invalidateEntries = reader.readBitSet("invalidateEntries");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                nearKeys = reader.readCollection("nearKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                ownedKeys = reader.readObjectArray("ownedKeys", MessageCollectionItemType.MSG, KeyCacheObject.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                ownedValues = reader.readObjectArray("ownedValues", MessageCollectionItemType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                preloadKeys = reader.readBitSet("preloadKeys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtLockRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 30;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 30;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockRequest.class, this, "super", super.toString());
    }
}