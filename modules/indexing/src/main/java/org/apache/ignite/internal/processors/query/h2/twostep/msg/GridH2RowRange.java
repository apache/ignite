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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Range of rows.
 */
public class GridH2RowRange implements Message {
    /** */
    private static int FLAG_PARTIAL = 1;

    /** */
    private int rangeId;

    /** */
    @GridDirectCollection(Message.class)
    @GridToStringInclude
    private List<GridH2RowMessage> rows;

    /** */
    private byte flags;

    /**
     * @param rangeId Range ID.
     */
    public void rangeId(int rangeId) {
        this.rangeId = rangeId;
    }

    /**
     * @return Range ID.
     */
    public int rangeId() {
        return rangeId;
    }

    /**
     * @param rows Rows.
     */
    public void rows(List<GridH2RowMessage> rows) {
        this.rows = rows;
    }

    /**
     * @return Rows.
     */
    public List<GridH2RowMessage> rows() {
        return rows;
    }

    /**
     * Sets that this is a partial range.
     */
    public void setPartial() {
        flags |= FLAG_PARTIAL;
    }

    /**
     * @return {@code true} If this is a partial range.
     */
    public boolean isPartial() {
        return (flags & FLAG_PARTIAL) == FLAG_PARTIAL;
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("rangeId", rangeId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("rows", rows, MessageCollectionItemType.MSG))
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
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                rangeId = reader.readInt("rangeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                rows = reader.readCollection("rows", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2RowRange.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -34;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2RowRange.class, this, "rowsSize", rows != null ? rows.size() : 0);
    }
}
