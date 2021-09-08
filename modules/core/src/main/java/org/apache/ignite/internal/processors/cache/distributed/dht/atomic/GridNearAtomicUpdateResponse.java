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
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache near update response.
 */
public class GridNearAtomicUpdateResponse extends GridCacheIdMessage implements GridCacheDeployable {
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

    /** Data for near cache update. */
    private NearCacheUpdates nearUpdates;

    /** Partition ID. */
    private int partId = -1;

    /** */
    @GridDirectCollection(UUID.class)
    @GridToStringInclude
    private List<UUID> mapping;

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
     * @param mapping Mapping.
     */
    public void mapping(List<UUID> mapping) {
        this.mapping = mapping;
    }

    /**
     * @return DHT nodes.
     */
    @Nullable public List<UUID> mapping() {
        return mapping;
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
    public void error(IgniteCheckedException err) {
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
     *
     */
    private void initNearUpdates() {
        if (nearUpdates == null)
            nearUpdates = new NearCacheUpdates();
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
        initNearUpdates();

        nearUpdates.addNearValue(keyIdx, val, ttl, expireTime);
    }

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    void addNearTtl(int keyIdx, long ttl, long expireTime) {
        initNearUpdates();

        nearUpdates.addNearTtl(keyIdx, ttl, expireTime);
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public long nearExpireTime(int idx) {
        return nearUpdates != null ? nearUpdates.nearExpireTime(idx) : -1L;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public long nearTtl(int idx) {
        return nearUpdates != null ? nearUpdates.nearTtl(idx) : -1L;
    }

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    void nearVersion(GridCacheVersion nearVer) {
        initNearUpdates();

        nearUpdates.nearVersion(nearVer);
    }

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    public GridCacheVersion nearVersion() {
        return nearUpdates != null ? nearUpdates.nearVersion() : null;
    }

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    void addSkippedIndex(int keyIdx) {
        initNearUpdates();

        nearUpdates.addSkippedIndex(keyIdx);
    }

    /**
     * @return Indexes of keys for which update was skipped
     */
    @Nullable public List<Integer> skippedIndexes() {
        return nearUpdates != null ? nearUpdates.skippedIndexes() : null;
    }

    /**
     * @return Indexes of keys for which values were generated on primary node.
     */
   @Nullable public List<Integer> nearValuesIndexes() {
        return nearUpdates != null ? nearUpdates.nearValuesIndexes() : null;
   }

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable public CacheObject nearValue(int idx) {
        return nearUpdates != null ? nearUpdates.nearValue(idx) : null;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKey(KeyCacheObject key, Throwable e) {
        assert key != null;
        assert e != null;

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

        if (nearUpdates != null)
            prepareMarshalCacheObjects(nearUpdates.nearValues(), cctx);

        if (ret != null)
            ret.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errs != null)
            errs.finishUnmarshal(this, cctx, ldr);

        if (nearUpdates != null)
            finishUnmarshalCacheObjects(nearUpdates.nearValues(), cctx, ldr);

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
            case 4:
                if (!writer.writeMessage("errs", errs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("mapping", mapping, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("nearUpdates", nearUpdates))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeAffinityTopologyVersion("remapTopVer", remapTopVer))
                    return false;

                writer.incrementState();

            case 10:
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
                mapping = reader.readCollection("mapping", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                nearUpdates = reader.readMessage("nearUpdates");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                remapTopVer = reader.readAffinityTopologyVersion("remapTopVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicUpdateResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 41;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateResponse.class, this, super.toString());
    }
}
