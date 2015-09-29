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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Continuous query entry.
 */
public class CacheContinuousQueryLostPartition extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Routine ID. */
    private UUID routineId;

    /** Partition. */
    private int part;

    /**
     * Required by {@link Message}.
     */
    public CacheContinuousQueryLostPartition() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param part Partition ID.
     */
    CacheContinuousQueryLostPartition(UUID routineId, int cacheId, int part) {
        this.routineId = routineId;
        this.cacheId = cacheId;
        this.part = part;
    }

    /**
     * @return Partition.
     */
    int partition() {
        return part;
    }

    /**
     * @return Routine ID.
     */
    UUID routineId() {
        return routineId;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 115;
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
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 4:
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                routineId = reader.readUuid("routineId");

                if (!reader.isLastRead())
                    return false;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryLostPartition.class, this);
    }
}
