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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery.EMPTY_PARAMS;

/** */
public class GridH2DmlRequest  implements Message, GridCacheQueryMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    @GridToStringInclude
    @GridDirectCollection(Integer.class)
    private List<Integer> caches;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Explicit partitions mappings for nodes. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = int[].class)
    private Map<UUID, int[]> parts;

    /** Query partitions. */
    @GridToStringInclude
    private int[] qryParts;

    /** */
    private int pageSize;

    /** */
    @GridToStringInclude
    @GridDirectCollection(Message.class)
    private List<GridCacheSqlQuery> qrys;

    /** */
    private byte flags;

    /** */
    @GridToStringInclude
    @GridDirectCollection(Message.class)
    private Collection<QueryTable> tbls;

    /** */
    private int timeout;

    /** */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    private Object[] params;

    /** */
    private byte[] paramsBytes;

    /** Schema name. */
    private String schemaName;

    /** Update mode. */
    private byte mode;

    /** Target table. */
    private String tgtTbl; //TODO: go for QueryTable instead?

    /** Target table column names. */
    private String[] colNames;

    /**
     * Required by {@link Externalizable}
     */
    public GridH2DmlRequest() {
        // No-op.
    }

    /**
     * @param req Request.
     */
    public GridH2DmlRequest(GridH2DmlRequest req) {
        reqId = req.reqId;
        caches = req.caches;
        topVer = req.topVer;
        parts = req.parts;
        qryParts = req.qryParts;
        pageSize = req.pageSize;
        qrys = req.qrys;
        flags = req.flags;
        tbls = req.tbls;
        timeout = req.timeout;
        params = req.params;
        paramsBytes = req.paramsBytes;
        schemaName = req.schemaName;
        mode = req.mode;
        tgtTbl = req.tgtTbl;
        colNames = req.colNames;
    }

    /**
     * @return Parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /**
     * @param params Parameters.
     * @return {@code this}.
     */
    public GridH2DmlRequest parameters(Object[] params) {
        if (params == null)
            params = EMPTY_PARAMS;

        this.params = params;

        return this;
    }

    /**
     * @param tbls Tables.
     * @return {@code this}.
     */
    public GridH2DmlRequest tables(Collection<QueryTable> tbls) {
        this.tbls = tbls;

        return this;
    }

    /**
     * @return Tables.
     */
    public Collection<QueryTable> tables() {
        return tbls;
    }

    /**
     * @param reqId Request ID.
     * @return {@code this}.
     */
    public GridH2DmlRequest requestId(long reqId) {
        this.reqId = reqId;

        return this;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param caches Caches.
     * @return {@code this}.
     */
    public GridH2DmlRequest caches(List<Integer> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * @return Caches.
     */
    public List<Integer> caches() {
        return caches;
    }

    /**
     * @param topVer Topology version.
     * @return {@code this}.
     */
    public GridH2DmlRequest topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;

        return this;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Explicit partitions mapping.
     */
    public Map<UUID, int[]> partitions() {
        return parts;
    }

    /**
     * @param parts Explicit partitions mapping.
     * @return {@code this}.
     */
    public GridH2DmlRequest partitions(Map<UUID, int[]> parts) {
        this.parts = parts;

        return this;
    }

    /**
     * @return Query partitions.
     */
    public int[] queryPartitions() {
        return qryParts;
    }

    /**
     * @param qryParts Query partitions.
     * @return {@code this}.
     */
    public GridH2DmlRequest queryPartitions(int[] qryParts) {
        this.qryParts = qryParts;

        return this;
    }

    /**
     * @param pageSize Page size.
     * @return {@code this}.
     */
    public GridH2DmlRequest pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param qrys SQL Queries.
     * @return {@code this}.
     */
    public GridH2DmlRequest queries(List<GridCacheSqlQuery> qrys) {
        this.qrys = qrys;

        return this;
    }

    /**
     * @return SQL Queries.
     */
    public List<GridCacheSqlQuery> queries() {
        return qrys;
    }

    /**
     * @param flags Flags.
     * @return {@code this}.
     */
    public GridH2DmlRequest flags(int flags) {
        assert flags >= 0 && flags <= 255: flags;

        this.flags = (byte)flags;

        return this;
    }

    /**
     * @param flags Flags to check.
     * @return {@code true} If all the requested flags are set to {@code true}.
     */
    public boolean isFlagSet(int flags) {
        return (this.flags & flags) == flags;
    }

    /**
     * @return Timeout.
     */
    public int timeout() {
        return timeout;
    }

    /**
     * @param timeout New timeout.
     * @return {@code this}.
     */
    public GridH2DmlRequest timeout(int timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return {@code this}.
     */
    public GridH2DmlRequest schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Update mode.
     */
    public byte mode() {
        return mode;
    }

    /**
     * @param mode Update mode.
     * @return {@code this}
     */
    public GridH2DmlRequest mode(byte mode) {
        this.mode = mode;

        return this;
    }

    /**
     * @return Target table.
     */
    public String targetTable() {
        return tgtTbl;
    }

    /**
     * @param targetTable Target table.
     * @return {@code this}
     */
    public GridH2DmlRequest targetTable(String targetTable) {
        tgtTbl = targetTable;

        return this;
    }

    /**
     * @return Target table column names.
     */
    public String[] columnNames() {
        return colNames;
    }

    /**
     * @param columnNames Target table column names.
     * @return {@code this}
     */
    public GridH2DmlRequest columnNames(String[] columnNames) {
        this.colNames = columnNames;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void marshall(Marshaller m) {
        if (paramsBytes != null)
            return;

        assert params != null;

        try {
            paramsBytes = U.marshal(m, params);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public void unmarshall(Marshaller m, GridKernalContext ctx) {
        if (params != null)
            return;

        assert paramsBytes != null;

        try {
            final ClassLoader ldr = U.resolveClassLoader(ctx.config());

            if (m instanceof BinaryMarshaller)
                // To avoid deserializing of enum types.
                params = ((BinaryMarshaller)m).binaryMarshaller().unmarshal(paramsBytes, ldr);
            else
                params = U.unmarshal(m, paramsBytes, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
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
                if (!writer.writeCollection("caches", caches, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeObjectArray("colNames", colNames, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByte("mode", mode))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap("parts", parts, MessageCollectionItemType.UUID, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeIntArray("qryParts", qryParts))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection("qrys", qrys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeString("schemaName", schemaName))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection("tbls", tbls, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeString("tgtTbl", tgtTbl))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeInt("timeout", timeout))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("topVer", topVer))
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
                caches = reader.readCollection("caches", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                colNames = reader.readObjectArray("colNames", MessageCollectionItemType.STRING, String.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                mode = reader.readByte("mode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                parts = reader.readMap("parts", MessageCollectionItemType.UUID, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                qryParts = reader.readIntArray("qryParts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                qrys = reader.readCollection("qrys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                schemaName = reader.readString("schemaName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                tbls = reader.readCollection("tbls", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                tgtTbl = reader.readString("tgtTbl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                timeout = reader.readInt("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2DmlRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -55;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2DmlRequest.class, this);
    }
}
