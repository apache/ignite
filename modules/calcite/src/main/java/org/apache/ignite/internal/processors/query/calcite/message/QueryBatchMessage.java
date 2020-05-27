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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class QueryBatchMessage implements MarshalableMessage, ExecutionContextAware {
    /** */
    private UUID qryId;

    /** */
    private long fragmentId;

    /** */
    private long exchangeId;

    /** */
    private int batchId;

    /** */
    private boolean last;

    /** */
    @GridDirectTransient
    private List<Object> rows;

    /** */
    @GridDirectCollection(Message.class)
    private List<Message> mRows;

    /** */
    public QueryBatchMessage() {
    }

    /** */
    public QueryBatchMessage(UUID qryId, long fragmentId, long exchangeId, int batchId, boolean last, List<Object> rows) {
        this.qryId = qryId;
        this.fragmentId = fragmentId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.last = last;
        this.rows = rows;
    }

    /** {@inheritDoc} */
    @Override public UUID queryId() {
        return qryId;
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

    /**
     * @return Last batch flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Rows.
     */
    public List<Object> rows() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) throws IgniteCheckedException {
        if (mRows != null || rows == null)
            return;

        mRows = new ArrayList<>(rows.size());

        for (Object row : rows) {
            Message mRow = CalciteMessageFactory.asMessage(row);

            if (mRow instanceof MarshalableMessage)
                ((MarshalableMessage) mRow).prepareMarshal(marshaller);

            mRows.add(mRow);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) throws IgniteCheckedException {
        if (rows != null || mRows == null)
            return;

        rows = new ArrayList<>(mRows.size());

        for (Message mRow : mRows) {
            if (mRow instanceof MarshalableMessage)
                ((MarshalableMessage) mRow).prepareUnmarshal(marshaller, loader);

            Object row = CalciteMessageFactory.asRow(mRow);

            rows.add(row);
        }
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
                if (!writer.writeInt("batchId", batchId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("exchangeId", exchangeId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("fragmentId", fragmentId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeBoolean("last", last))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("mRows", mRows, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeUuid("queryId", qryId))
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
                batchId = reader.readInt("batchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                exchangeId = reader.readLong("exchangeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                fragmentId = reader.readLong("fragmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                last = reader.readBoolean("last");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                mRows = reader.readCollection("mRows", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                qryId = reader.readUuid("queryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(QueryBatchMessage.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_BATCH_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }
}
