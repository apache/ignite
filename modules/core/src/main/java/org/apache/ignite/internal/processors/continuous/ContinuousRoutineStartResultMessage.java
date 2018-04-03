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

package org.apache.ignite.internal.processors.continuous;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ContinuousRoutineStartResultMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int ERROR_FLAG = 0x01;

    /** */
    private UUID routineId;

    /** */
    private byte[] errBytes;

    /** */
    private byte[] cntrsMapBytes;

    /** */
    private int flags;

    /**
     *
     */
    public ContinuousRoutineStartResultMessage() {
        // No-op.
    }

    /**
     * @param routineId Routine ID.
     * @param cntrsMapBytes Marshalled {@link CachePartitionPartialCountersMap}.
     * @param errBytes Error bytes.
     * @param err {@code True} if failed to start routine.
     */
    ContinuousRoutineStartResultMessage(UUID routineId, byte[] cntrsMapBytes, byte[] errBytes, boolean err) {
        this.routineId = routineId;
        this.cntrsMapBytes = cntrsMapBytes;
        this.errBytes = errBytes;

        if (err)
            flags |= ERROR_FLAG;
    }

    /**
     * @return Marshalled {@link CachePartitionPartialCountersMap}.
     */
    @Nullable byte[] countersMapBytes() {
        return cntrsMapBytes;
    }

    /**
     * @return {@code True} if failed to start routine.
     */
    boolean error() {
        return (flags & ERROR_FLAG) != 0;
    }

    /**
     * @return Routine ID.
     */
    UUID routineId() {
        return routineId;
    }

    /**
     * @return Error bytes.
     */
    @Nullable byte[] errorBytes() {
        return errBytes;
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
                if (!writer.writeByteArray("cntrsMapBytes", cntrsMapBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("flags", flags))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid("routineId", routineId))
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
                cntrsMapBytes = reader.readByteArray("cntrsMapBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                flags = reader.readInt("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                routineId = reader.readUuid("routineId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ContinuousRoutineStartResultMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 134;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinuousRoutineStartResultMessage.class, this);
    }
}
