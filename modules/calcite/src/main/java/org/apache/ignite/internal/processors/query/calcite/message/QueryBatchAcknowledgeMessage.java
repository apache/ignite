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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class QueryBatchAcknowledgeMessage implements ExecutionContextAware {
    /** */
    private UUID queryId;

    /** */
    private long fragmentId;

    /** */
    private long exchangeId;

    /** */
    private int batchId;

    /** */
    public QueryBatchAcknowledgeMessage() {

    }

    /** */
    public QueryBatchAcknowledgeMessage(UUID queryId, long fragmentId, long exchangeId, int batchId) {
        this.queryId = queryId;
        this.fragmentId = fragmentId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
    }

    /** {@inheritDoc} */
    @Override public UUID queryId() {
        return queryId;
    }

    /** {@inheritDoc} */
    @Override public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * @return Batch ID.
     */
    public int batchId() {
        return batchId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(batchId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong(exchangeId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong(fragmentId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid(queryId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                batchId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                exchangeId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                fragmentId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                queryId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_ACKNOWLEDGE_MESSAGE;
    }
}
