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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message containing changes which should be applied only on MVCC transaction commit.
 */
public class DeferredPartitionUpdates implements Message {
    /** Map of update counters made by this tx. Mapping: partId -> updCntr. */
    @GridDirectMap(keyType = Integer.class, valueType = Long.class)
    private Map<Integer, Long> updCntrs = new HashMap<>();
    /** Partition size changes by this tx. */
    @GridDirectMap(keyType = Integer.class, valueType = Long.class)
    private Map<Integer, Long> sizeDeltas = new HashMap<>();

    /**
     * @return Partition update counters.
     */
    public Map<Integer, Long> updateCounters() {
        return updCntrs;
    }

    /**
     * @return Partition size deltas.
     */
    public Map<Integer, Long> sizeDeltas() {
        return sizeDeltas;
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
                if (!writer.writeMap("updCntrs", updCntrs, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();
            case 1:
                if (!writer.writeMap("sizeDeltas", updCntrs, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
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
                updCntrs = reader.readMap("updCntrs", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
            case 1:
                sizeDeltas = reader.readMap("sizeDeltas", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(DeferredPartitionUpdates.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 157;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
