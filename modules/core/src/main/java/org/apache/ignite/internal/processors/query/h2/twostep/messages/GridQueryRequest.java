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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 * Query request.
 */
public class GridQueryRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    private int pageSize;

    /** */
    private String space;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheSqlQuery> qrys;

    /** */
    private byte[] qrysBytes;

    /**
     * Default constructor.
     */
    public GridQueryRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param pageSize Page size.
     * @param space Space.
     * @param qrys Queries.
     * @param qrysBytes Marshalled queries.
     */
    public GridQueryRequest(long reqId, int pageSize, String space, Collection<GridCacheSqlQuery> qrys, byte[] qrysBytes) {
        this.reqId = reqId;
        this.pageSize = pageSize;
        this.space = space;

        assert qrysBytes != null;

        this.qrys = qrys;
        this.qrysBytes = qrysBytes;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
    }

    /**
     * @return Queries.
     */
    public Collection<GridCacheSqlQuery> queries(Marshaller m) throws IgniteCheckedException {
        if (qrys == null && qrysBytes != null)
            qrys = m.unmarshal(qrysBytes, null);

        return qrys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryRequest.class, this);
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
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("qrysBytes", qrysBytes))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeString("space", space))
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
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                qrysBytes = reader.readByteArray("qrysBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                space = reader.readString("space");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 110;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}
