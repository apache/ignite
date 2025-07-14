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

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Request to fetch next page.
 */
public class GridQueryNextPageRequest implements Message {
    /** */
    private long qryReqId;

    /** */
    private int segmentId;

    /** */
    private int qry;

    /** */
    private int pageSize;

    /** */
    private byte flags;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public GridQueryNextPageRequest() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     * @param flags Flags.
     */
    public GridQueryNextPageRequest(long qryReqId, int qry, int segmentId, int pageSize, byte flags) {
        this.qryReqId = qryReqId;
        this.qry = qry;
        this.segmentId = segmentId;
        this.pageSize = pageSize;
        this.flags = flags;
    }

    /**
     * @return Flags.
     */
    public byte getFlags() {
        return flags;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Query.
     */
    public int query() {
        return qry;
    }

    /** @return Index segment ID */
    public int segmentId() {
        return segmentId;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryNextPageRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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
                if (!writer.writeByte(flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt(pageSize))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt(qry))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong(qryReqId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeInt(segmentId))
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
                flags = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                pageSize = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                qry = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qryReqId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                segmentId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 108;
    }
}
