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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Batch acknowledgement.
 */
public class CacheContinuousQueryBatchAck extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Routine ID. */
    private UUID routineId;

    /** Update indexes. */
    @GridToStringInclude
    @GridDirectMap(keyType = Integer.class, valueType = Long.class)
    private Map<Integer, Long> updateIdxs;

    /**
     * Default constructor.
     */
    public CacheContinuousQueryBatchAck() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param routineId Routine ID.
     * @param updateIdxs Update indexes.
     */
    CacheContinuousQueryBatchAck(int cacheId, UUID routineId, Map<Integer, Long> updateIdxs) {
        this.cacheId = cacheId;
        this.routineId = routineId;
        this.updateIdxs = updateIdxs;
    }

    /**
     * @return Routine ID.
     */
    UUID routineId() {
        return routineId;
    }

    /**
     * @return Update indexes.
     */
    Map<Integer, Long> updateIndexes() {
        return updateIdxs;
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
                if (!writer.writeUuid("routineId", routineId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap("updateIdxs", updateIdxs, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
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
                routineId = reader.readUuid("routineId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                updateIdxs = reader.readMap("updateIdxs", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 114;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryBatchAck.class, this);
    }
}
