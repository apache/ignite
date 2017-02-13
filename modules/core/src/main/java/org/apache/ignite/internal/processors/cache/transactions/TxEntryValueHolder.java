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

package org.apache.ignite.internal.processors.cache.transactions;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Auxiliary class to hold value, value-has-been-set flag, value update operation, value bytes.
 */
@IgniteCodeGeneratingFail // Need to handle 'hasWriteVal' flag during write.
public class TxEntryValueHolder implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private CacheObject val;

    /** */
    @GridToStringInclude
    private GridCacheOperation op = NOOP;

    /** Flag indicating that value has been set for write. */
    private boolean hasWriteVal;

    /** Flag indicating that value has been set for read. */
    @GridDirectTransient
    private boolean hasReadVal;

    /**
     * @param op Cache operation.
     * @param val Value.
     * @param hasWriteVal Write value presence flag.
     * @param hasReadVal Read value presence flag.
     */
    public void value(GridCacheOperation op, CacheObject val, boolean hasWriteVal, boolean hasReadVal) {
        if (hasReadVal && this.hasWriteVal)
            return;

        this.op = op;
        this.val = val;

        this.hasWriteVal = hasWriteVal || op == CREATE || op == UPDATE || op == DELETE;
        this.hasReadVal = hasReadVal || op == READ;
    }

    /**
     * @return {@code True} if has read or write value.
     */
    public boolean hasValue() {
        return hasWriteVal || hasReadVal;
    }

    /**
     * Gets stored value.
     *
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val Stored value.
     */
    public void value(@Nullable CacheObject val) {
        this.val = val;
    }

    /**
     * Gets cache operation.
     *
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return op;
    }

    /**
     * Sets cache operation.
     *
     * @param op Cache operation.
     */
    public void op(GridCacheOperation op) {
        this.op = op;
    }

    /**
     * @return {@code True} if write value was set.
     */
    public boolean hasWriteValue() {
        return hasWriteVal;
    }

    /**
     * @return {@code True} if read value was set.
     */
    public boolean hasReadValue() {
        return hasReadVal;
    }

    /**
     * @param ctx Cache context.
     * @throws org.apache.ignite.IgniteCheckedException If marshaling failed.
     */
    public void marshal(GridCacheContext<?, ?> ctx)
        throws IgniteCheckedException {
        if (hasWriteVal && val != null)
            val.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws org.apache.ignite.IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(GridCacheContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (hasWriteVal && val != null)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "[op=" + op +", val=" + val + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean("hasWriteVal", hasWriteVal))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("val", hasWriteVal ? val : null))
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
                hasWriteVal = reader.readBoolean("hasWriteVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 2:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(TxEntryValueHolder.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 101;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }
}
