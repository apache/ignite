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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Reply for synchronous phase 2.
 */
public class GridNearTxFinishResponse extends GridDistributedTxFinishResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Heuristic error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near tx thread ID. */
    private long nearThreadId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearTxFinishResponse() {
        // No-op.
    }

    /**
     * @param xid Xid version.
     * @param nearThreadId Near tx thread ID.
     * @param futId Future ID.
     * @param miniId Mini future Id.
     * @param err Error.
     */
    public GridNearTxFinishResponse(GridCacheVersion xid, long nearThreadId, IgniteUuid futId, IgniteUuid miniId,
        @Nullable Throwable err) {
        super(xid, futId);

        assert miniId != null;

        this.nearThreadId = nearThreadId;
        this.miniId = miniId;
        this.err = err;
    }

    /**
     * @return Error.
     */
    @Nullable public Throwable error() {
        return err;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Near thread ID.
     */
    public long threadId() {
        return nearThreadId;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);
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
            case 5:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("nearThreadId", nearThreadId))
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
            case 5:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                nearThreadId = reader.readLong("nearThreadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxFinishResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 54;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxFinishResponse.class, this, "super", super.toString());
    }
}
