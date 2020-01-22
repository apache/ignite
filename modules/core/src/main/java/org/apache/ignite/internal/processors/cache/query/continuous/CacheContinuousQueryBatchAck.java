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
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Batch acknowledgement.
 */
public class CacheContinuousQueryBatchAck extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Routine ID. */
    private UUID routineId;

    /** Update counters. */
    @GridToStringInclude
    @GridDirectMap(keyType = Integer.class, valueType = Long.class)
    private Map<Integer, Long> updateCntrs;

    /**
     * Default constructor.
     */
    public CacheContinuousQueryBatchAck() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param routineId Routine ID.
     * @param updateCntrs Update counters.
     */
    CacheContinuousQueryBatchAck(int cacheId, UUID routineId, Map<Integer, Long> updateCntrs) {
        this.cacheId = cacheId;
        this.routineId = routineId;
        this.updateCntrs = updateCntrs;
    }

    /**
     * @return Routine ID.
     */
    UUID routineId() {
        return routineId;
    }

    /**
     * @return Update counters.
     */
    Map<Integer, Long> updateCntrs() {
        return updateCntrs;
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
                if (!writer.writeUuid("routineId", routineId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap("updateCntrs", updateCntrs, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
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
                routineId = reader.readUuid("routineId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                updateCntrs = reader.readMap("updateCntrs", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheContinuousQueryBatchAck.class);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 118;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryBatchAck.class, this);
    }
}
