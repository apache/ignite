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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Message that holds a transaction message and Consistent Cut info.
 */
public class ConsistentCutAwareMessage extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 400;

    /** Original transaction message. */
    private GridCacheMessage payload;

    /** Consistent Cut ID. */
    private UUID cutId;

    /** ID of the latest Consistent Cut AFTER which this transaction committed. */
    private @Nullable UUID txCutId;

    /** */
    public ConsistentCutAwareMessage() {
    }

    /** */
    public ConsistentCutAwareMessage(
        GridCacheMessage payload,
        UUID cutId,
        @Nullable UUID txCutId
    ) {
        this.payload = payload;
        this.cutId = cutId;
        this.txCutId = txCutId;
    }

    /** */
    public UUID cutId() {
        return cutId;
    }

    /** */
    public UUID txCutId() {
        return txCutId;
    }

    /** */
    public GridCacheMessage payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        payload.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        payload.finishUnmarshal(ctx, ldr);
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
                if (!writer.writeUuid("cutId", cutId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("payload", payload))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeUuid("txCutId", txCutId))
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
                cutId = reader.readUuid("cutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                payload = reader.readMessage("payload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                txCutId = reader.readUuid("txCutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ConsistentCutAwareMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }
}
