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
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 * Query request.
 */
@IgniteCodeGeneratingFail
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
    @GridDirectCollection(GridCacheSqlQuery.class)
    private Collection<GridCacheSqlQuery> qrys;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** */
    @GridToStringInclude
    @GridDirectCollection(String.class)
    private List<String> extraSpaces;

    /** */
    @GridToStringInclude
    private int[] parts;

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
     * @param topVer Topology version.
     * @param extraSpaces All space names participating in query other than {@code space}.
     * @param parts Optional partitions for unstable topology.
     */
    public GridQueryRequest(
        long reqId,
        int pageSize,
        String space,
        Collection<GridCacheSqlQuery> qrys,
        AffinityTopologyVersion topVer,
        List<String> extraSpaces,
        int[] parts) {
        this.reqId = reqId;
        this.pageSize = pageSize;
        this.space = space;

        this.qrys = qrys;
        this.topVer = topVer;
        this.extraSpaces = extraSpaces;
        this.parts = parts;
    }

    /**
     * @param cp Copy from.
     */
    public GridQueryRequest(GridQueryRequest cp) {
        this.reqId = cp.reqId;
        this.pageSize = cp.pageSize;
        this.space = cp.space;
        this.qrys = cp.qrys;
        this.topVer = cp.topVer;
        this.extraSpaces = cp.extraSpaces;
        this.parts = cp.parts;
    }

    /**
     * @return All the needed partitions for {@link #space()} and {@link #extraSpaces()}.
     */
    public int[] partitions() {
        return parts;
    }

    /**
     * @param parts All the needed partitions for {@link #space()} and {@link #extraSpaces()}.
     */
    public void partitions(int[] parts) {
        this.parts = parts;
    }

    /**
     * @return All extra space names participating in query other than {@link #space()}.
     */
    public List<String> extraSpaces() {
        return extraSpaces;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
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
    public Collection<GridCacheSqlQuery> queries() throws IgniteCheckedException {
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
                if (!writer.writeCollection("qrys", qrys, MessageCollectionItemType.MSG))
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

            case 4:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("extraSpaces", extraSpaces, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIntArray("parts", parts))
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
                qrys = reader.readCollection("qrys", MessageCollectionItemType.MSG);

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

            case 4:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                extraSpaces = reader.readCollection("extraSpaces", MessageCollectionItemType.STRING);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                parts = reader.readIntArray("parts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridQueryRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 110;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }
}
