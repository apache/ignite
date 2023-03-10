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
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Message that holds a transaction message and incremental snapshot ID.
 */
public class IncrementalSnapshotAwareMessage extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 400;

    /** Original transaction message. */
    private GridCacheMessage payload;

    /** Incremental snapshot ID. */
    private UUID id;

    /** ID of the latest incremental snapshot after which this transaction committed. */
    private @Nullable UUID txSnpId;

    /** Incremental snapshot topology version. */
    private long topVer;

    /** */
    public IncrementalSnapshotAwareMessage() {
    }

    /** */
    public IncrementalSnapshotAwareMessage(
        GridCacheMessage payload,
        UUID id,
        @Nullable UUID txSnpId,
        long topVer
    ) {
        this.payload = payload;
        this.id = id;
        this.txSnpId = txSnpId;
        this.topVer = topVer;
    }

    /** @return Incremental snapshot ID. */
    public UUID id() {
        return id;
    }

    /** ID of the latest incremental snapshot after which this transaction committed. */
    public UUID txInrementalSnapshotId() {
        return txSnpId;
    }

    /** */
    public GridCacheMessage payload() {
        return payload;
    }

    /** @return Incremental snapshot topology version. */
    public long snapshotTopologyVersion() {
        return topVer;
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
            case 3:
                if (!writer.writeUuid("id", id))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("payload", payload))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeUuid("txSnpId", txSnpId))
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
                id = reader.readUuid("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                payload = reader.readMessage("payload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                txSnpId = reader.readUuid("txSnpId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IncrementalSnapshotAwareMessage.class);
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

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }
}
