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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Bounds of row range.
 */
public class GridH2RowRangeBounds implements Message {
    /** */
    private int rangeId;

    /** */
    private GridH2RowMessage first;

    /** */
    private GridH2RowMessage last;

    /**
     * @param rangeId Range ID.
     * @param first First.
     * @param last Last.
     * @return Range bounds.
     */
    public static GridH2RowRangeBounds rangeBounds(int rangeId, GridH2RowMessage first, GridH2RowMessage last) {
        GridH2RowRangeBounds res = new GridH2RowRangeBounds();

        res.rangeId(rangeId);
        res.first(first);
        res.last(last);

        return res;
    }

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
     * @param first First.
     */
    public void first(GridH2RowMessage first) {
        this.first = first;
    }

    /**
     * @return First.
     */
    public GridH2RowMessage first() {
        return first;
    }

    /**
     * @param last Last.
     */
    public void last(GridH2RowMessage last) {
        this.last = last;
    }

    /**
     * @return Last.
     */
    public GridH2RowMessage last() {
        return last;
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
                if (!writer.writeMessage("first", first))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("last", last))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("rangeId", rangeId))
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
                first = reader.readMessage("first");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                last = reader.readMessage("last");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                rangeId = reader.readInt("rangeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2RowRangeBounds.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -35;
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
        return S.toString(GridH2RowRangeBounds.class, this);
    }
}
