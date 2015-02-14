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
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT atomic cache backup update response.
 */
public class GridDhtAtomicUpdateResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future version. */
    private GridCacheVersion futVer;

    /** Failed keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> failedKeys;

    /** Serialized failed keys. */
    private byte[] failedKeysBytes;

    /** Update error. */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** Serialized update error. */
    private byte[] errBytes;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> nearEvicted;

    /** Evicted reader key bytes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearEvictedBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futVer Future version.
     */
    public GridDhtAtomicUpdateResponse(int cacheId, GridCacheVersion futVer) {
        this.cacheId = cacheId;
        this.futVer = futVer;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Gets update error.
     */
    public IgniteCheckedException error() {
        return err;
    }

    /**
     * @return Failed keys.
     */
    public Collection<K> failedKeys() {
        return failedKeys;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public void addFailedKey(K key, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>();

        failedKeys.add(key);

        if (err == null)
            err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /**
     * @return Evicted readers.
     */
    public Collection<K> nearEvicted() {
        return nearEvicted;
    }

    /**
     * Adds near evicted key..
     *
     * @param key Evicted key.
     * @param bytes Bytes of evicted key.
     */
    public void addNearEvicted(K key, @Nullable byte[] bytes) {
        if (nearEvicted == null)
            nearEvicted = new ArrayList<>();

        nearEvicted.add(key);

        if (bytes != null) {
            if (nearEvictedBytes == null)
                nearEvictedBytes = new ArrayList<>();

            nearEvictedBytes.add(bytes);
        }
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        failedKeysBytes = ctx.marshaller().marshal(failedKeys);
        errBytes = ctx.marshaller().marshal(err);

        if (nearEvictedBytes == null)
            nearEvictedBytes = marshalCollection(nearEvicted, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        failedKeys = ctx.marshaller().unmarshal(failedKeysBytes, ldr);
        err = ctx.marshaller().unmarshal(errBytes, ldr);

        if (nearEvicted == null && nearEvictedBytes != null)
            nearEvicted = unmarshalCollection(nearEvictedBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAtomicUpdateResponse _clone = (GridDhtAtomicUpdateResponse)_msg;

        _clone.futVer = futVer != null ? (GridCacheVersion)futVer.clone() : null;
        _clone.failedKeys = failedKeys;
        _clone.failedKeysBytes = failedKeysBytes;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.nearEvicted = nearEvicted;
        _clone.nearEvictedBytes = nearEvictedBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 3:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeByteArray("failedKeysBytes", failedKeysBytes))
                    return false;

                state.increment();

            case 5:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                state.increment();

            case 6:
                if (!writer.writeCollection("nearEvictedBytes", nearEvictedBytes, byte[].class))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
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
                nearEvictedBytes = reader.readCollection("nearEvictedBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 39;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateResponse.class, this);
    }
}
