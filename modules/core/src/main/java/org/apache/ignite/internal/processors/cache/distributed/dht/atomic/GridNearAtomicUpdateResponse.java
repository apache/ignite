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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * DHT atomic cache near update response.
 */
public class GridNearAtomicUpdateResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
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
    @GridDirectTransient
    private GridCacheReturn<Object> retVal;

    /** Serialized return value. */
    private byte[] retValBytes;

    /** Failed keys. */
    @GridToStringInclude
    @GridDirectTransient
    private volatile Collection<K> failedKeys;

    /** Serialized failed keys. */
    private byte[] failedKeysBytes;

    /** Keys that should be remapped. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> remapKeys;

    /** Serialized keys that should be remapped. */
    private byte[] remapKeysBytes;

    /** Indexes of keys for which values were generated on primary node (used if originating node has near cache). */
    @GridDirectCollection(int.class)
    private List<Integer> nearValsIdxs;

    /** Indexes of keys for which update was skipped (used if originating node has near cache). */
    @GridDirectCollection(int.class)
    private List<Integer> nearSkipIdxs;

    /** Values generated on primary node which should be put to originating node's near cache. */
    @GridToStringInclude
    @GridDirectTransient
    private List<V> nearVals;

    /** Serialized values generated on primary node which should be put to originating node's near cache. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> nearValBytes;

    /** Version generated on primary node to be used for originating node's near cache update. */
    private GridCacheVersion nearVer;

    /** Near TTLs. */
    private GridLongList nearTtls;

    /** Near expire times. */
    private GridLongList nearExpireTimes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID this reply should be sent to.
     * @param futVer Future version.
     */
    public GridNearAtomicUpdateResponse(int cacheId, UUID nodeId, GridCacheVersion futVer) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
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
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Update error, if any.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @return Collection of failed keys.
     */
    public Collection<K> failedKeys() {
        return failedKeys;
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn<Object> returnValue() {
        return retVal;
    }

    /**
     * @param retVal Return value.
     */
    public void returnValue(GridCacheReturn<Object> retVal) {
        this.retVal = retVal;
    }

    /**
     * @param remapKeys Remap keys.
     */
    public void remapKeys(Collection<K> remapKeys) {
        this.remapKeys = remapKeys;
    }

    /**
     * @return Remap keys.
     */
    public Collection<K> remapKeys() {
        return remapKeys;
    }

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    public void addNearValue(int keyIdx,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime) {
        if (nearValsIdxs == null) {
            nearValsIdxs = new ArrayList<>();
            nearValBytes = new ArrayList<>();
            nearVals = new ArrayList<>();
        }

        addNearTtl(keyIdx, ttl, expireTime);

        nearValsIdxs.add(keyIdx);
        nearVals.add(val);
        nearValBytes.add(valBytes != null ? GridCacheValueBytes.marshaled(valBytes) : null);
    }

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void addNearTtl(int keyIdx, long ttl, long expireTime) {
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
    public void nearVersion(GridCacheVersion nearVer) {
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
    public void addSkippedIndex(int keyIdx) {
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
    @Nullable public V nearValue(int idx) {
        return nearVals.get(idx);
    }

    /**
     * @param idx Index.
     * @return Serialized value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable public byte[] nearValueBytes(int idx) {
        if (nearValBytes != null) {
            GridCacheValueBytes valBytes0 = nearValBytes.get(idx);

            if (valBytes0 != null && !valBytes0.isPlain())
                return valBytes0.get();
        }

        return null;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKey(K key, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ConcurrentLinkedQueue<>();

        failedKeys.add(key);

        if (err == null)
            err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKeys(Collection<K> keys, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>(keys.size());

        failedKeys.addAll(keys);

        if (err == null)
            err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);

        if (retVal != null)
            retValBytes = ctx.marshaller().marshal(retVal);

        if (failedKeys != null)
            failedKeysBytes = ctx.marshaller().marshal(failedKeys);

        if (remapKeys != null)
            remapKeysBytes = ctx.marshaller().marshal(remapKeys);

        nearValBytes = marshalValuesCollection(nearVals, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);

        if (retValBytes != null)
            retVal = ctx.marshaller().unmarshal(retValBytes, ldr);

        if (failedKeysBytes != null)
            failedKeys = ctx.marshaller().unmarshal(failedKeysBytes, ldr);

        if (remapKeysBytes != null)
            remapKeys = ctx.marshaller().unmarshal(remapKeysBytes, ldr);

        nearVals = unmarshalValueBytesCollection(nearValBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridNearAtomicUpdateResponse _clone = (GridNearAtomicUpdateResponse)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer != null ? (GridCacheVersion)futVer.clone() : null;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.retVal = retVal;
        _clone.retValBytes = retValBytes;
        _clone.failedKeys = failedKeys;
        _clone.failedKeysBytes = failedKeysBytes;
        _clone.remapKeys = remapKeys;
        _clone.remapKeysBytes = remapKeysBytes;
        _clone.nearValsIdxs = nearValsIdxs;
        _clone.nearSkipIdxs = nearSkipIdxs;
        _clone.nearVals = nearVals;
        _clone.nearValBytes = nearValBytes;
        _clone.nearVer = nearVer != null ? (GridCacheVersion)nearVer.clone() : null;
        _clone.nearTtls = nearTtls != null ? (GridLongList)nearTtls.clone() : null;
        _clone.nearExpireTimes = nearExpireTimes != null ? (GridLongList)nearExpireTimes.clone() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("failedKeysBytes", failedKeysBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("nearExpireTimes", nearExpireTimes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("nearSkipIdxs", nearSkipIdxs, Type.INT))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("nearTtls", nearTtls))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("nearValBytes", nearValBytes, Type.MSG))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection("nearValsIdxs", nearValsIdxs, Type.INT))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("nearVer", nearVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeByteArray("remapKeysBytes", remapKeysBytes))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByteArray("retValBytes", retValBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                failedKeysBytes = reader.readByteArray("failedKeysBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                nearExpireTimes = reader.readMessage("nearExpireTimes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                nearSkipIdxs = reader.readCollection("nearSkipIdxs", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                nearTtls = reader.readMessage("nearTtls");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                nearValBytes = reader.readCollection("nearValBytes", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                nearValsIdxs = reader.readCollection("nearValsIdxs", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                nearVer = reader.readMessage("nearVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                remapKeysBytes = reader.readByteArray("remapKeysBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                retValBytes = reader.readByteArray("retValBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 41;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateResponse.class, this, "parent");
    }
}
