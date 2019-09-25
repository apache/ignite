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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest.DHT_ATOMIC_HAS_RESULT_MASK;

/**
 * Message sent from DHT nodes to near node in FULL_SYNC mode.
 */
public class GridDhtAtomicNearResponse extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    private int partId;

    /** */
    private long futId;

    /** */
    private UUID primaryId;

    /** */
    @GridToStringExclude
    private byte flags;

    /** */
    @GridToStringInclude
    private UpdateErrors errs;

    /**
     *
     */
    public GridDhtAtomicNearResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition.
     * @param futId Future ID.
     * @param primaryId Primary node ID.
     * @param flags Flags.
     */
    public GridDhtAtomicNearResponse(int cacheId,
        int partId,
        long futId,
        UUID primaryId,
        byte flags)
    {
        assert primaryId != null;

        this.cacheId = cacheId;
        this.partId = partId;
        this.futId = futId;
        this.primaryId = primaryId;
        this.flags = flags;
    }

    /**
     * @return Errors.
     */
    @Nullable UpdateErrors errors() {
        return errs;
    }

    /**
     * @param errs Errors.
     */
    public void errors(UpdateErrors errs) {
        this.errs = errs;
    }

    /**
     * @return Primary node ID.
     */
    UUID primaryId() {
        return primaryId;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /**
     * @param key Key.
     * @param e Error.
     */
    public void addFailedKey(KeyCacheObject key, Throwable e) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.addFailedKey(key, e);
    }

    /**
     * @return Operation result.
     */
    public GridCacheReturn result() {
        assert hasResult() : this;

        return new GridCacheReturn(true, true);
    }

    /**
     * @return {@code True} if response contains operation result.
     */
    boolean hasResult() {
        return isFlag(DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -48;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (errs != null)
            errs.prepareMarshal(this, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errs != null)
            errs.finishUnmarshal(this, ctx.cacheContext(cacheId), ldr);
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
                if (!writer.writeMessage("errs", errs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeUuid("primaryId", primaryId))
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
                errs = reader.readMessage("errs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                primaryId = reader.readUuid("primaryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicNearResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (hasResult())
            appendFlag(flags, "hasRes");

        return S.toString(GridDhtAtomicNearResponse.class, this,
            "flags", flags.toString());
    }
}
