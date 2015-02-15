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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Lock request message.
 */
public class GridDistributedUnlockRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Keys to unlock. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Keys. */
    @GridDirectTransient
    private List<K> keys;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDistributedUnlockRequest() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
     * @param keyCnt Key count.
     */
    public GridDistributedUnlockRequest(int cacheId, int keyCnt) {
        super(keyCnt);

        this.cacheId = cacheId;
    }

    /**
     * @return Key to lock.
     */
    public List<byte[]> keyBytes() {
        return keyBytes;
    }

    /**
     * @return Keys.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @param key Key.
     * @param bytes Key bytes.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addKey(K key, byte[] bytes, GridCacheContext<K, V> ctx) throws IgniteCheckedException {
        boolean depEnabled = ctx.deploymentEnabled();

        if (depEnabled)
            prepareObject(key, ctx.shared());

        if (keys == null)
            keys = new ArrayList<>(keysCount());

        keys.add(key);

        if (keyBytes == null)
            keyBytes = new ArrayList<>(keysCount());

        keyBytes.add(bytes);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (F.isEmpty(keyBytes) && !F.isEmpty(keys))
            keyBytes = marshalCollection(keys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null && !F.isEmpty(keyBytes))
            keys = unmarshalCollection(keyBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedUnlockRequest _clone = (GridDistributedUnlockRequest)_msg;

        _clone.keyBytes = keyBytes;
        _clone.keys = keys;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
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
            case 8:
                if (!writer.writeCollection("keyBytes", keyBytes, Type.BYTE_ARR))
                    return false;

                writer.incrementState();

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
            case 8:
                keyBytes = reader.readCollection("keyBytes", Type.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedUnlockRequest.class, this, "keyBytesSize",
            keyBytes == null ? 0 : keyBytes.size(), "super", super.toString());
    }
}
