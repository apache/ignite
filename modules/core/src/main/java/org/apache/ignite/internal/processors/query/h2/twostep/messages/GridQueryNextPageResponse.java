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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Next page response.
 */
@IgniteCodeGeneratingFail
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
    private int cols;

    /** */
    @GridDirectCollection(Message.class)
    private Collection<Message> vals;

    /** */
    @GridDirectTransient
    private transient Collection<?> plainRows;

    /** */
    private AffinityTopologyVersion retry;

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
     * @param cols Number of columns in row.
     * @param vals Values for rows in this page added sequentially.
     * @param plainRows Not marshalled rows for local node.
     */
    public GridQueryNextPageResponse(long qryReqId, int qry, int page, int allRows, int cols,
        Collection<Message> vals, Collection<?> plainRows) {
        assert vals != null ^ plainRows != null;
        assert cols > 0 : cols;

        this.qryReqId = qryReqId;
        this.qry = qry;
        this.page = page;
        this.allRows = allRows;
        this.cols = cols;
        this.vals = vals;
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
     * @return Columns in row.
     */
    public int columns() {
        return cols;
    }

    /**
     * @return Values.
     */
    public Collection<Message> values() {
        return vals;
    }

    /**
     * @return Plain rows.
     */
    public Collection<?> plainRows() {
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
                if (!writer.writeInt("cols", cols))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("page", page))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("qry", qry))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("qryReqId", qryReqId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("retry", retry))
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
                cols = reader.readInt("cols");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                page = reader.readInt("page");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qry = reader.readInt("qry");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                qryReqId = reader.readLong("qryReqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                retry = reader.readMessage("retry");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridQueryNextPageResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 109;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /**
     * @return Retry topology version.
     */
    public AffinityTopologyVersion retry() {
        return retry;
    }

    /**
     * @param retry Retry topology version.
     */
    public void retry(AffinityTopologyVersion retry) {
        this.retry = retry;
    }
}