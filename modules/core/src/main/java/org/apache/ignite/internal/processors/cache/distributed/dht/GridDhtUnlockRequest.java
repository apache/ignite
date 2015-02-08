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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT cache unlock request.
 */
public class GridDhtUnlockRequest<K, V> extends GridDistributedUnlockRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near keys. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearKeyBytes;

    /** */
    @GridDirectTransient
    private List<K> nearKeys;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtUnlockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param dhtCnt Key count.
     */
    public GridDhtUnlockRequest(int cacheId, int dhtCnt) {
        super(cacheId, dhtCnt);
    }

    /**
     * @return Near keys.
     */
    public List<byte[]> nearKeyBytes() {
        return nearKeyBytes != null ? nearKeyBytes : Collections.<byte[]>emptyList();
    }

    /**
     * @return Near keys.
     */
    public List<K> nearKeys() {
        return nearKeys;
    }

    /**
     * Adds a Near key.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addNearKey(K key, byte[] keyBytes, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        if (ctx.deploymentEnabled())
            prepareObject(key, ctx);

        if (nearKeyBytes == null)
            nearKeyBytes = new ArrayList<>();

        nearKeyBytes.add(keyBytes);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearKeyBytes != null)
            nearKeys = unmarshalCollection(nearKeyBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtUnlockRequest.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtUnlockRequest _clone = new GridDhtUnlockRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtUnlockRequest _clone = (GridDhtUnlockRequest)_msg;

        _clone.nearKeyBytes = nearKeyBytes;
        _clone.nearKeys = nearKeys;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 9:
                if (!writer.writeCollection("nearKeyBytes", nearKeyBytes, byte[].class))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 9:
                nearKeyBytes = reader.readCollection("nearKeyBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 36;
    }
}
