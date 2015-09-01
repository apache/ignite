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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Cache eviction response.
 */
public class GridCacheEvictionResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private long futId;

    /** Rejected keys. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private Collection<KeyCacheObject> rejectedKeys = new HashSet<>();

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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(rejectedKeys, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(rejectedKeys, ctx.cacheContext(cacheId), ldr);
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
    Collection<KeyCacheObject> rejectedKeys() {
        return rejectedKeys;
    }

    /**
     * Add rejected key to response.
     *
     * @param key Evicted key.
     */
    void addRejected(KeyCacheObject key) {
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
                if (!writer.writeBoolean("err", err))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("rejectedKeys", rejectedKeys, MessageCollectionItemType.MSG))
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
                err = reader.readBoolean("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                rejectedKeys = reader.readCollection("rejectedKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheEvictionResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionResponse.class, this);
    }
}