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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Cache eviction response.
 */
public class GridCacheEvictionResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private long futId;

    /** Rejected keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> rejectedKeys = new HashSet<>();

    /** Serialized rejected keys. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> rejectedKeyBytes;

    /** Flag to indicate whether request processing has finished with error. */
    private boolean err;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     */
    GridCacheEvictionResponse(int cacheId, long futId) {
        this(cacheId, futId, false);
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param err {@code True} if request processing has finished with error.
     */
    GridCacheEvictionResponse(int cacheId, long futId, boolean err) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.err = err;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        rejectedKeyBytes = marshalCollection(rejectedKeys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        rejectedKeys = unmarshalCollection(rejectedKeyBytes, ctx, ldr);
    }

    /**
     * @return Future ID.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Rejected keys.
     */
    Collection<K> rejectedKeys() {
        return rejectedKeys;
    }

    /**
     * Add rejected key to response.
     *
     * @param key Evicted key.
     */
    void addRejected(K key) {
        assert key != null;

        rejectedKeys.add(key);
    }

    /**
     * @return {@code True} if request processing has finished with error.
     */
    boolean error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridCacheEvictionResponse _clone = new GridCacheEvictionResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheEvictionResponse _clone = (GridCacheEvictionResponse)_msg;

        _clone.futId = futId;
        _clone.rejectedKeys = rejectedKeys;
        _clone.rejectedKeyBytes = rejectedKeyBytes;
        _clone.err = err;
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
            case 3:
                if (!writer.writeBoolean("err", err))
                    return false;

                state++;

            case 4:
                if (!writer.writeLong("futId", futId))
                    return false;

                state++;

            case 5:
                if (!writer.writeCollection("rejectedKeyBytes", rejectedKeyBytes, byte[].class))
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
            case 3:
                err = reader.readBoolean("err");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 5:
                rejectedKeyBytes = reader.readCollection("rejectedKeyBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionResponse.class, this);
    }
}
