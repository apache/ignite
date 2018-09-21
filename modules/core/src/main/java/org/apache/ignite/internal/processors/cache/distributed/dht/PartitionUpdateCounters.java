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
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Partition update counters message.
 */
public class PartitionUpdateCounters implements Message {
    /** */
    private static final long serialVersionUID = 193442457510062844L;

    /** Map of update counters made by this tx. Mapping: partId -> updCntr. */
    @GridDirectMap(keyType = Integer.class, valueType = Long.class)
    private Map<Integer, Long> updCntrs;

    /** */
    public PartitionUpdateCounters() {
        // No-op.
    }

    /**
     * @param updCntrs Update counters map.
     */
    public PartitionUpdateCounters(Map<Integer, Long> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /**
     * @return Update counters.
     */
    public Map<Integer, Long> updateCounters() {
        return updCntrs;
    }

    /**
     * @param updCntrs Update counters.
     */
    public void updateCounters(Map<Integer, Long> updCntrs) {
        this.updCntrs = updCntrs;
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

        }

        return reader.afterMessageRead(PartitionUpdateCounters.class);
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
