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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class ErrorMessage implements MarshalableMessage {
    /** */
    private UUID queryId;

    /** */
    private long fragmentId;

    /** */
    private byte[] errBytes;

    /** */
    @GridDirectTransient
    private Throwable err;

    /** */
    public ErrorMessage() {
        // No-op.
    }

    /** */
    public ErrorMessage(UUID queryId, long fragmentId, Throwable err) {
        assert err != null;

        this.queryId = queryId;
        this.fragmentId = fragmentId;
        this.err = err;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return queryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Marshaled Throwable.
     */
    public Throwable error() {
        assert err != null;

        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("fragmentId", fragmentId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("queryId", queryId))
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

        switch (reader.state()) {
            case 0:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                fragmentId = reader.readLong("fragmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                queryId = reader.readUuid("queryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ErrorMessage.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_ERROR_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        errBytes = U.marshal(ctx, err);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (errBytes != null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ctx.gridConfig()));
    }
}
