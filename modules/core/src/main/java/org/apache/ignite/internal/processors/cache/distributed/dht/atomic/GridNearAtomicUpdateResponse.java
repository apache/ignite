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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache near update response.
 */
public class GridNearAtomicUpdateResponse extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID this reply should be sent to. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future ID. */
    private long futId;

    /** */
    private UpdateErrors errs;

    /** Return value. */
    @GridToStringInclude
    private GridCacheReturn ret;

    /** */
    private AffinityTopologyVersion remapTopVer;

    /** Indexes of keys for which values were generated on primary node (used if originating node has near cache). */
    @GridDirectCollection(int.class)
    private List<Integer> nearValsIdxs;

    /** Indexes of keys for which update was skipped (used if originating node has near cache). */
    @GridDirectCollection(int.class)
    private List<Integer> nearSkipIdxs;

    /** Values generated on primary node which should be put to originating node's near cache. */
    @GridToStringInclude
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> nearVals;

    /** Version generated on primary node to be used for originating node's near cache update. */
    private GridCacheVersion nearVer;

    /** Near TTLs. */
    private GridLongList nearTtls;

    /** Near expire times. */
    private GridLongList nearExpireTimes;

    /** Partition ID. */
    private int partId = -1;

    /** */
    @GridDirectCollection(UUID.class)
    @GridToStringInclude
    private List<UUID> dhtNodes;

    /** */
    @GridDirectTransient
    private boolean nodeLeft;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID this reply should be sent to.
     * @param futId Future ID.
     * @param partId Partition.
     * @param nodeLeft {@code True} if primary node failed.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearAtomicUpdateResponse(int cacheId,
        UUID nodeId,
        long futId,
        int partId,
        boolean nodeLeft,
        boolean addDepInfo) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futId = futId;
        this.partId = partId;
        this.nodeLeft = nodeLeft;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @return {@code True} if primary node failed.
     */
    public boolean nodeLeftResponse() {
        return nodeLeft;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @param dhtNodes DHT nodes.
     */
    public void dhtNodes(List<UUID> dhtNodes) {
        this.dhtNodes = dhtNodes;
    }

    /**
     * @return DHT nodes.
     */
    @Nullable public List<UUID> dhtNodes() {
        return dhtNodes;
    }

    /**
     * @return Node ID this response should be sent to.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    public void error(IgniteCheckedException err){
        if (errs == null)
            errs = new UpdateErrors();

        errs.onError(err);
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return errs != null ? errs.error() : null;
    }

    /**
     * @return Collection of failed keys.
     */
    public Collection<KeyCacheObject> failedKeys() {
        return errs != null ? errs.failedKeys() : null;
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn returnValue() {
        return ret;
    }

    /**
     * @param ret Return value.
     */
    @SuppressWarnings("unchecked")
    public void returnValue(GridCacheReturn ret) {
        this.ret = ret;
    }

    /**
     * @param remapTopVer Topology version to remap update.
     */
    void remapTopologyVersion(AffinityTopologyVersion remapTopVer) {
        this.remapTopVer = remapTopVer;
    }

    /**
     * @return Topology version if update should be remapped.
     */
    @Nullable AffinityTopologyVersion remapTopologyVersion() {
        return remapTopVer;
    }

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    void addNearValue(int keyIdx,
        @Nullable CacheObject val,
        long ttl,
        long expireTime) {
        if (nearValsIdxs == null) {
            nearValsIdxs = new ArrayList<>();
            nearVals = new ArrayList<>();
        }

        addNearTtl(keyIdx, ttl, expireTime);

        nearValsIdxs.add(keyIdx);
        nearVals.add(val);
    }

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void addNearTtl(int keyIdx, long ttl, long expireTime) {
        if (ttl >= 0) {
            if (nearTtls == null) {
                nearTtls = new GridLongList(16);

                for (int i = 0; i < keyIdx; i++)
                    nearTtls.add(-1L);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0) {
            if (nearExpireTimes == null) {
                nearExpireTimes = new GridLongList(16);

                for (int i = 0; i < keyIdx; i++)
                    nearExpireTimes.add(-1);
            }
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return -1L;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
        }

        return -1L;
    }

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    void nearVersion(GridCacheVersion nearVer) {
        this.nearVer = nearVer;
    }

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    public GridCacheVersion nearVersion() {
        return nearVer;
    }

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    void addSkippedIndex(int keyIdx) {
        if (nearSkipIdxs == null)
            nearSkipIdxs = new ArrayList<>();

        nearSkipIdxs.add(keyIdx);

        addNearTtl(keyIdx, -1L, -1L);
    }

    /**
     * @return Indexes of keys for which update was skipped
     */
    @Nullable public List<Integer> skippedIndexes() {
        return nearSkipIdxs;
    }

    /**
     * @return Indexes of keys for which values were generated on primary node.
     */
   @Nullable public List<Integer> nearValuesIndexes() {
        return nearValsIdxs;
   }

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable public CacheObject nearValue(int idx) {
        return nearVals.get(idx);
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKey(KeyCacheObject key, Throwable e) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.addFailedKey(key, e);
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    synchronized void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.addFailedKeys(keys, e);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errs != null)
            errs.prepareMarshal(this, cctx);

        prepareMarshalCacheObjects(nearVals, cctx);

        if (ret != null)
            ret.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errs != null)
            errs.finishUnmarshal(this, cctx, ldr);

        finishUnmarshalCacheObjects(nearVals, cctx, ldr);

        if (ret != null)
            ret.finishUnmarshal(cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
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
            case 3:
                if (!writer.writeCollection("dhtNodes", dhtNodes, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("errs", errs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("nearExpireTimes", nearExpireTimes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("nearSkipIdxs", nearSkipIdxs, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("nearTtls", nearTtls))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("nearVals", nearVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection("nearValsIdxs", nearValsIdxs, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("nearVer", nearVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("remapTopVer", remapTopVer))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("ret", ret))
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
            case 3:
                dhtNodes = reader.readCollection("dhtNodes", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                errs = reader.readMessage("errs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                nearExpireTimes = reader.readMessage("nearExpireTimes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                nearSkipIdxs = reader.readCollection("nearSkipIdxs", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                nearTtls = reader.readMessage("nearTtls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                nearVals = reader.readCollection("nearVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                nearValsIdxs = reader.readCollection("nearValsIdxs", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                nearVer = reader.readMessage("nearVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                remapTopVer = reader.readMessage("remapTopVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicUpdateResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 41;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateResponse.class, this, "parent");
    }
}
