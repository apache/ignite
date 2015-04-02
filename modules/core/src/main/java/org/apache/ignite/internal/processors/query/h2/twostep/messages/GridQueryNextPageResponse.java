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

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;

/**
 * Next page response.
 */
public class GridQueryNextPageResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long qryReqId;

    /** */
    private int qry;

    /** */
    private int page;

    /** */
    private int allRows;

    /** */
    private byte[] rows;

    /** */
    @GridDirectTransient
    private transient Object plainRows;

    /**
     * For {@link Externalizable}.
     */
    public GridQueryNextPageResponse() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param page Page.
     * @param allRows All rows count.
     * @param rows Rows.
     * @param plainRows Not marshalled rows for local node.
     */
    public GridQueryNextPageResponse(long qryReqId, int qry, int page, int allRows,
        byte[] rows, Object plainRows) {
        assert rows != null ^ plainRows != null;

        this.qryReqId = qryReqId;
        this.qry = qry;
        this.page = page;
        this.allRows = allRows;
        this.rows = rows;
        this.plainRows = plainRows;
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

    /**
     * @return Page.
     */
    public int page() {
        return page;
    }

    /**
     * @return All rows.
     */
    public int allRows() {
        return allRows;
    }

    /**
     * @return Rows.
     */
    public byte[] rows() {
        return rows;
    }

    /**
     * @return Plain rows.
     */
    public Object plainRows() {
        return plainRows;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryNextPageResponse.class, this);
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
                if (!writer.writeInt("allRows", allRows))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("page", page))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("qry", qry))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("qryReqId", qryReqId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("rows", rows))
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
                allRows = reader.readInt("allRows");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                page = reader.readInt("page");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                qry = reader.readInt("qry");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qryReqId = reader.readLong("qryReqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                rows = reader.readByteArray("rows");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 109;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }
}
