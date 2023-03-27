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
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.QueryTable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery.EMPTY_PARAMS;

/**
 * Query request.
 */
@IgniteCodeGeneratingFail
public class GridH2QueryRequest implements Message, GridCacheQueryMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Map query will not destroy context until explicit query cancel request will be received because distributed join
     * requests can be received.
     */
    public static final int FLAG_DISTRIBUTED_JOINS = 1;

    /**
     * Remote map query executor will enforce join order for the received map queries.
     */
    public static final int FLAG_ENFORCE_JOIN_ORDER = 1 << 1;

    /**
     * Whether to treat replicated as partitioned (for outer joins).
     */
    public static final int FLAG_REPLICATED_AS_PARTITIONED = 1 << 2;

    /**
     * If it is an EXPLAIN command.
     */
    public static final int FLAG_EXPLAIN = 1 << 3;

    /**
     * If it is a REPLICATED query.
     */
    public static final int FLAG_REPLICATED = 1 << 4;

    /**
     * If lazy execution is enabled.
     */
    public static final int FLAG_LAZY = 1 << 5;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_SHIFT = 6;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_MASK = 0b11 << FLAG_DATA_PAGE_SCAN_SHIFT;

    /** */
    @SuppressWarnings("PointlessBitwiseExpression")
    private static final int FLAG_DATA_PAGE_SCAN_DFLT = 0b00 << FLAG_DATA_PAGE_SCAN_SHIFT;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_ENABLED = 0b01 << FLAG_DATA_PAGE_SCAN_SHIFT;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_DISABLED = 0b10 << FLAG_DATA_PAGE_SCAN_SHIFT;

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

    /** */
    private MvccSnapshot mvccSnapshot;

    /** TX details holder for {@code SELECT FOR UPDATE}, or {@code null} if not applicable. */
    private GridH2SelectForUpdateTxDetails txReq;

    /** Id of the query assigned by {@link RunningQueryManager} on originator node. */
    private long qryId;

    /** */
    private boolean explicitTimeout;

    /**
     * Required by {@link Externalizable}
     */
    public GridH2QueryRequest() {
        // No-op.
    }

    /**
     * @param req Request.
     */
    public GridH2QueryRequest(GridH2QueryRequest req) {
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
        mvccSnapshot = req.mvccSnapshot;
        txReq = req.txReq;
        qryId = req.qryId;
        explicitTimeout = req.explicitTimeout;
    }

    /**
     * @return MVCC snapshot.
     */
    @Nullable public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @param mvccSnapshot MVCC snapshot version.
     * @return {@code this}.
     */
    public GridH2QueryRequest mvccSnapshot(MvccSnapshot mvccSnapshot) {
        this.mvccSnapshot = mvccSnapshot;

        return this;
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
    public GridH2QueryRequest parameters(Object[] params) {
        if (params == null)
            params = EMPTY_PARAMS;

        this.params = params;

        return this;
    }

    /**
     * @param tbls Tables.
     * @return {@code this}.
     */
    public GridH2QueryRequest tables(Collection<QueryTable> tbls) {
        this.tbls = tbls;

        return this;
    }

    /**
     * Get tables.
     * <p>
     * N.B.: Was used in AI 1.9 for snapshots. Unused at the moment, but should be kept for compatibility reasons.
     *
     * @return Tables.
     */
    public Collection<QueryTable> tables() {
        return tbls;
    }

    /**
     * @param reqId Request ID.
     * @return {@code this}.
     */
    public GridH2QueryRequest requestId(long reqId) {
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
    public GridH2QueryRequest caches(List<Integer> caches) {
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
    public GridH2QueryRequest topologyVersion(AffinityTopologyVersion topVer) {
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
    public GridH2QueryRequest partitions(Map<UUID, int[]> parts) {
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
    public GridH2QueryRequest queryPartitions(int[] qryParts) {
        this.qryParts = qryParts;

        return this;
    }

    /**
     * @param pageSize Page size.
     * @return {@code this}.
     */
    public GridH2QueryRequest pageSize(int pageSize) {
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
    public GridH2QueryRequest queries(List<GridCacheSqlQuery> qrys) {
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
    public GridH2QueryRequest flags(int flags) {
        assert flags >= 0 && flags <= 255 : flags;

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
    public GridH2QueryRequest timeout(int timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return {@code true} if query timeout is set explicitly.
     */
    public boolean explicitTimeout() {
        return explicitTimeout;
    }

    /**
     * @param explicitTimeout Explicit timeout flag.
     * @return {@code this}.
     */
    public GridH2QueryRequest explicitTimeout(boolean explicitTimeout) {
        this.explicitTimeout = explicitTimeout;

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
    public GridH2QueryRequest schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return TX details holder for {@code SELECT FOR UPDATE}, or {@code null} if not applicable.
     */
    public GridH2SelectForUpdateTxDetails txDetails() {
        return txReq;
    }

    /**
     * @param txReq TX details holder for {@code SELECT FOR UPDATE}, or {@code null} if not applicable.
     */
    public void txDetails(GridH2SelectForUpdateTxDetails txReq) {
        this.txReq = txReq;
    }

    /**
     * @param flags Flags.
     * @param dataPageScanEnabled {@code true} If data page scan enabled, {@code false} if not, and {@code null} if not set.
     * @return Updated flags.
     */
    public static int setDataPageScanEnabled(int flags, Boolean dataPageScanEnabled) {
        int x = dataPageScanEnabled == null ? FLAG_DATA_PAGE_SCAN_DFLT :
            dataPageScanEnabled ? FLAG_DATA_PAGE_SCAN_ENABLED : FLAG_DATA_PAGE_SCAN_DISABLED;

        flags &= ~FLAG_DATA_PAGE_SCAN_MASK; // Clear old bits.
        flags |= x; // Set new bits.

        return flags;
    }

    /**
     * Build query flags.
     *
     * @return  Query flags.
     */
    public static int queryFlags(boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean lazy,
        boolean replicatedOnly,
        boolean explain,
        Boolean dataPageScanEnabled,
        boolean treatReplicatedAsPartitioned) {
        int flags = enforceJoinOrder ? FLAG_ENFORCE_JOIN_ORDER : 0;

        // Distributed joins flag is set if it is either reald
        if (distributedJoins)
            flags |= FLAG_DISTRIBUTED_JOINS;

        if (explain)
            flags |= FLAG_EXPLAIN;

        if (replicatedOnly)
            flags |= FLAG_REPLICATED;

        if (lazy)
            flags |= FLAG_LAZY;

        flags = setDataPageScanEnabled(flags, dataPageScanEnabled);

        if (treatReplicatedAsPartitioned)
            flags |= FLAG_REPLICATED_AS_PARTITIONED;

        return flags;
    }

    /**
     * Id of the query assigned by {@link RunningQueryManager} on originator node.
     *
     * @return Query id.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * Sets id of the query assigned by {@link RunningQueryManager}.
     *
     * @param queryId Query id.
     * @return {@code this} for chaining.
     */
    public GridH2QueryRequest queryId(long queryId) {
        this.qryId = queryId;

        return this;
    }

    /**
     * Checks if data page scan enabled.
     *
     * @return {@code true} If data page scan enabled, {@code false} if not, and {@code null} if not set.
     */
    public Boolean isDataPageScanEnabled() {
        return isDataPageScanEnabled(flags);
    }

    /**
     * Checks if data page scan enabled.
     *
     * @param flags Flags.
     * @return {@code true} If data page scan enabled, {@code false} if not, and {@code null} if not set.
     */
    public static Boolean isDataPageScanEnabled(int flags) {
        switch (flags & FLAG_DATA_PAGE_SCAN_MASK) {
            case FLAG_DATA_PAGE_SCAN_ENABLED:
                return true;

            case FLAG_DATA_PAGE_SCAN_DISABLED:
                return false;
        }

        return null;
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
        assert paramsBytes != null;

        try {
            final ClassLoader ldr = U.resolveClassLoader(ctx.config());

            if (m instanceof BinaryMarshaller)
                // To avoid deserializing of enum types.
                params = BinaryUtils.rawArrayFromBinary(((BinaryMarshaller)m).binaryMarshaller().unmarshal(paramsBytes, ldr));
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap("parts", parts, MessageCollectionItemType.UUID, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("qrys", qrys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("tbls", tbls, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("timeout", timeout))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeIntArray("qryParts", qryParts))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeString("schemaName", schemaName))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("txReq", txReq))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeBoolean("explicitTimeout", explicitTimeout))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeLong("qryId", qryId))
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
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                parts = reader.readMap("parts", MessageCollectionItemType.UUID, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                qrys = reader.readCollection("qrys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                tbls = reader.readCollection("tbls", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                timeout = reader.readInt("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                qryParts = reader.readIntArray("qryParts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                schemaName = reader.readString("schemaName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                txReq = reader.readMessage("txReq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                explicitTimeout = reader.readBoolean("explicitTimeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                qryId = reader.readLong("qryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridH2QueryRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -33;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2QueryRequest.class, this);
    }
}
