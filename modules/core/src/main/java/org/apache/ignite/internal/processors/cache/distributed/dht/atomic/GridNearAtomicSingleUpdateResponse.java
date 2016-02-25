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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class GridNearAtomicSingleUpdateResponse extends GridCacheMessage implements GridCacheDeployable, GridNearAtomicUpdateResponseInterface {

    /** */
    private static final long serialVersionUID = 0L;

    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID this reply should be sent to. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Update error. */
    @GridDirectTransient
    private volatile IgniteCheckedException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Return value. */
    @GridToStringInclude
    private GridCacheReturn ret;

    /** Key. */
    private KeyCacheObject key;

    /** Failed. */
    private boolean failed;

    /** Remap. */
    private boolean remap;

    /** Has near value. */
    private boolean hasNearVal;

    /** Near value. */
    private CacheObject nearVal;

    /** Skipped near update. */
    private boolean nearSkip;

    /** Near TTL. */
    private long nearTtl = -1;

    /** Near expire time. */
    private long nearExpireTime = -1;

    /** Version generated on primary node to be used for originating node's near cache update. */
    private GridCacheVersion nearVer;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID this reply should be sent to.
     * @param futVer Future version.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearAtomicSingleUpdateResponse(int cacheId, UUID nodeId, GridCacheVersion futVer, boolean addDepInfo) {
        assert futVer != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID this response should be sent to.
     */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    @Override public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Future version.
     */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    @Override public void error(IgniteCheckedException err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return err;
    }

    /**
     * @return Collection of failed keys.
     */
    @Override public Collection<KeyCacheObject> failedKeys() {
        if (failed && key != null)
            return Collections.singletonList(key);

        return null;
    }

    /**
     * @return Return value.
     */
    @Override public GridCacheReturn returnValue() {
        return ret;
    }

    /**
     * @param ret Return value.
     */
    @Override @SuppressWarnings("unchecked")
    public void returnValue(GridCacheReturn ret) {
        this.ret = ret;
    }

    /**
     * @param remapKeys Remap keys.
     */
    @Override public void remapKeys(List<KeyCacheObject> remapKeys) {
        assert remapKeys.size() <= 1;

        if (remapKeys.isEmpty())
            return;

        key = remapKeys.get(0);
        remap = true;
    }

    /**
     * @return Remap keys.
     */
    @Override public Collection<KeyCacheObject> remapKeys() {
        if (remap && key != null)
            return Collections.singletonList(key);

        return null;
    }

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    @Override public void addNearValue(int keyIdx,
        @Nullable CacheObject val,
        long ttl,
        long expireTime) {

        assert keyIdx == 0;

        nearVal = val;
        hasNearVal = true;

        addNearTtl(keyIdx, ttl, expireTime);
    }

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    @Override @SuppressWarnings("ForLoopReplaceableByForEach")
    public void addNearTtl(int keyIdx, long ttl, long expireTime) {
        assert keyIdx == 0;

        nearTtl = ttl >= 0 ? ttl : -1;

        nearExpireTime = expireTime >= 0 ? expireTime : -1;
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    @Override public long nearExpireTime(int idx) {
        if (idx == 0)
            return nearExpireTime;

        return -1L;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    @Override public long nearTtl(int idx) {
        if (idx == 0)
            return nearTtl;

        return -1L;
    }

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    @Override public void nearVersion(GridCacheVersion nearVer) {
        this.nearVer = nearVer;
    }

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    @Override public GridCacheVersion nearVersion() {
        return nearVer;
    }

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    @Override public void addSkippedIndex(int keyIdx) {
        assert keyIdx == 0;

        nearSkip = true;
        nearTtl = -1;
        nearExpireTime = -1;
    }

    /**
     * @return Indexes of keys for which update was skipped
     */
    @Override @Nullable public List<Integer> skippedIndexes() {
        if (nearSkip)
            return Collections.singletonList(0);

        return null;
    }

    /**
     * @return Indexes of keys for which values were generated on primary node.
     */
    @Override @Nullable public List<Integer> nearValuesIndexes() {
        if (hasNearVal)
            return Collections.singletonList(0);

        return null;
    }

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Override @Nullable public CacheObject nearValue(int idx) {
        assert idx == 0;

        if (hasNearVal)
            return nearVal;

        return null;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    @Override public synchronized void addFailedKey(KeyCacheObject key, Throwable e) {
        this.key = key;
        failed = true;

        err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    @Override public synchronized void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e) {
        if (keys != null) {
            assert keys.size() <= 1;

            if (keys.size() == 1)
                key = F.first(keys);
        }

        err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     * @param ctx Context.
     */
    @Override public synchronized void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e,
        GridCacheContext ctx) {
        addFailedKeys(keys, e);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null && errBytes == null)
            errBytes = ctx.marshaller().marshal(err);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

        prepareMarshalCacheObject(nearVal, cctx);

        if (ret != null)
            ret.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null && err == null)
            err = ctx.marshaller().unmarshal(errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObject(key, cctx, ldr);

        finishUnmarshalCacheObject(nearVal, cctx, ldr);

        if (ret != null)
            ret.finishUnmarshal(cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
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
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeBoolean("failed", failed))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("nearExpireTime", nearExpireTime))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("nearSkip", nearSkip))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong("nearTtl", nearTtl))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("nearVal", nearVal))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("hasNearVal", hasNearVal))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("nearVer", nearVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 13:
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
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                failed = reader.readBoolean("failed");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                nearExpireTime = reader.readLong("nearExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                nearSkip = reader.readBoolean("nearSkip");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                nearTtl = reader.readLong("nearTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                nearVal = reader.readMessage("nearVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                hasNearVal = reader.readBoolean("hasNearVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                nearVer = reader.readMessage("nearVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicSingleUpdateResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -24;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 14;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateResponse.class, this, "parent");
    }

}
