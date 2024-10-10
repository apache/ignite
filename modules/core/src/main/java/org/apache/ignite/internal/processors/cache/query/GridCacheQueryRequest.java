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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.INDEX;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SPI;

/**
 * Query request.
 */
public class GridCacheQueryRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_DFLT = 0b00;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_ENABLED = 0b01;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_DISABLED = 0b10;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_MASK = 0b11;

    /** */
    private long id;

    /** */
    private String cacheName;

    /** */
    private GridCacheQueryType type;

    /** */
    private boolean fields;

    /** */
    @GridToStringInclude(sensitive = true)
    private String clause;

    /** */
    @GridDirectTransient
    private IndexQueryDesc idxQryDesc;

    /** */
    private byte[] idxQryDescBytes;

    /** */
    private int limit;

    /** */
    private String clsName;

    /** */
    @GridDirectTransient
    private IgniteBiPredicate<Object, Object> keyValFilter;

    /** */
    private byte[] keyValFilterBytes;

    /** */
    @GridDirectTransient
    private IgniteReducer<Object, Object> rdc;

    /** */
    private byte[] rdcBytes;

    /** */
    @GridDirectTransient
    private IgniteClosure<?, ?> trans;

    /** */
    private byte[] transBytes;

    /** */
    @GridDirectTransient
    private Object[] args;

    /** */
    private byte[] argsBytes;

    /** */
    private int pageSize;

    /** */
    private boolean incBackups;

    /** */
    private boolean cancel;

    /** */
    private boolean incMeta;

    /** */
    private boolean all;

    /** */
    private boolean keepBinary;

    /** */
    private int taskHash;

    /** Partition. */
    private int part = -1;

    /** */
    private AffinityTopologyVersion topVer;

    /** Set of keys that must be skiped during iteration. */
    private Set<KeyCacheObject> skipKeys;

    /** */
    private byte flags;

    /**
     * Required by {@link Externalizable}
     */
    public GridCacheQueryRequest() {
        // No-op.
    }

    /**
     * Send initial query request to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     * @param fut Cache query future, contains query info.
     */
    public static GridCacheQueryRequest startQueryRequest(GridCacheContext<?, ?> cctx, long reqId,
        GridCacheDistributedQueryFuture<?, ?, ?> fut) {
        GridCacheQueryBean bean = fut.query();
        GridCacheQueryAdapter<?> qry = bean.query();

        boolean deployFilterOrTransformer = (qry.scanFilter() != null || qry.transform() != null)
            && cctx.gridDeploy().enabled();

        return new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.type(),
            fut.fields(),
            qry.clause(),
            qry.idxQryDesc(),
            qry.limit(),
            qry.queryClassName(),
            qry.scanFilter(),
            qry.partition(),
            bean.reducer(),
            qry.transform(),
            qry.pageSize(),
            qry.includeBackups(),
            bean.arguments(),
            qry.includeMetadata(),
            qry.keepBinary(),
            qry.taskHash(),
            cctx.affinity().affinityTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || deployFilterOrTransformer,
            qry.isDataPageScanEnabled(),
            qry.skipKeys());
    }

    /**
     * Send request for fetching query result pages to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     */
    public static GridCacheQueryRequest pageRequest(GridCacheContext<?, ?> cctx, long reqId,
        GridCacheQueryAdapter<?> qry, boolean fields) {

        return new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.pageSize(),
            qry.includeBackups(),
            fields,
            false,
            qry.keepBinary(),
            qry.taskHash(),
            cctx.affinity().affinityTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || (qry.scanFilter() != null && cctx.gridDeploy().enabled()),
            qry.isDataPageScanEnabled());
    }

    /**
     * Send cancel query request, so no new pages will be sent.
     *
     * @param reqId Query request ID.
     * @param fieldsQry Whether query is a fields query.
     */
    public static GridCacheQueryRequest cancelRequest(GridCacheContext<?, ?> cctx, long reqId, boolean fieldsQry) {
        return new GridCacheQueryRequest(cctx.cacheId(),
            reqId,
            fieldsQry,
            cctx.affinity().affinityTopologyVersion(),
            cctx.deploymentEnabled());
    }

    /**
     * Creates cancel query request.
     *
     * @param cacheId Cache ID.
     * @param id Request to cancel.
     * @param fields Fields query flag.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        boolean fields,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.fields = fields;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;

        cancel = true;
    }

    /**
     * Request to load page.
     *
     * @param cacheId Cache ID.
     * @param id Request ID.
     * @param cacheName Cache name.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param fields Fields query flag.
     * @param all Whether to load all pages.
     * @param keepBinary Whether to keep binary.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     * @param dataPageScanEnabled Flag to enable data page scan.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        int pageSize,
        boolean incBackups,
        boolean fields,
        boolean all,
        boolean keepBinary,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Boolean dataPageScanEnabled
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.fields = fields;
        this.all = all;
        this.keepBinary = keepBinary;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;

        flags = setDataPageScanEnabled(flags, dataPageScanEnabled);
    }

    /**
     * Request to start query.
     *
     * @param cacheId Cache ID.
     * @param id Request id.
     * @param cacheName Cache name.
     * @param type Query type.
     * @param fields {@code true} if query returns fields.
     * @param clause Query clause.
     * @param limit Response limit. Set to 0 for no limits.
     * @param clsName Query class name.
     * @param keyValFilter Key-value filter.
     * @param part Partition.
     * @param rdc Reducer.
     * @param trans Transformer.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param args Query arguments.
     * @param incMeta Include meta data or not.
     * @param keepBinary Keep binary flag.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     * @param skipKeys Set of keys that must be skiped during iteration.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        GridCacheQueryType type,
        boolean fields,
        String clause,
        IndexQueryDesc idxQryDesc,
        int limit,
        String clsName,
        IgniteBiPredicate<Object, Object> keyValFilter,
        @Nullable Integer part,
        IgniteReducer<Object, Object> rdc,
        IgniteClosure<?, ?> trans,
        int pageSize,
        boolean incBackups,
        Object[] args,
        boolean incMeta,
        boolean keepBinary,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Boolean dataPageScanEnabled,
        @Nullable Set<KeyCacheObject> skipKeys
    ) {
        assert type != null || fields;
        assert clause != null || (type == SCAN || type == SET || type == SPI || type == INDEX);
        assert clsName != null || fields || type == SCAN || type == SET || type == SPI;

        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.type = type;
        this.fields = fields;
        this.clause = clause;
        this.idxQryDesc = idxQryDesc;
        this.limit = limit;
        this.clsName = clsName;
        this.keyValFilter = keyValFilter;
        this.part = part == null ? -1 : part;
        this.rdc = rdc;
        this.trans = trans;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.args = args;
        this.incMeta = incMeta;
        this.keepBinary = keepBinary;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
        this.skipKeys = skipKeys;

        flags = setDataPageScanEnabled(flags, dataPageScanEnabled);
    }

    /**
     * @param flags Flags.
     * @param enabled If data page scan enabled.
     * @return Updated flags.
     */
    private static byte setDataPageScanEnabled(int flags, Boolean enabled) {
        int x = enabled == null ? FLAG_DATA_PAGE_SCAN_DFLT :
            enabled ? FLAG_DATA_PAGE_SCAN_ENABLED : FLAG_DATA_PAGE_SCAN_DISABLED;

        flags &= ~FLAG_DATA_PAGE_SCAN_MASK; // Clear old bits.
        flags |= x; // Set new bits.

        return (byte)flags;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer != null ? topVer : AffinityTopologyVersion.NONE;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (keyValFilter != null && keyValFilterBytes == null) {
            if (addDepInfo)
                prepareObject(keyValFilter, cctx);

            keyValFilterBytes = CU.marshal(cctx, keyValFilter);
        }

        if (rdc != null && rdcBytes == null) {
            if (addDepInfo)
                prepareObject(rdc, cctx);

            rdcBytes = CU.marshal(cctx, rdc);
        }

        if (trans != null && transBytes == null) {
            if (addDepInfo)
                prepareObject(trans, cctx);

            transBytes = CU.marshal(cctx, trans);
        }

        if (!F.isEmpty(args) && argsBytes == null) {
            if (addDepInfo) {
                for (Object arg : args)
                    prepareObject(arg, cctx);
            }

            argsBytes = CU.marshal(cctx, args);
        }

        if (idxQryDesc != null && idxQryDescBytes == null) {
            if (addDepInfo)
                prepareObject(idxQryDesc, cctx);

            idxQryDescBytes = CU.marshal(cctx, idxQryDesc);
        }

        if (!F.isEmpty(skipKeys)) {
            for (KeyCacheObject k : skipKeys) {
                k.prepareMarshal(cctx.cacheObjectContext());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        Marshaller mrsh = ctx.marshaller();

        ClassLoader clsLdr = U.resolveClassLoader(ldr, ctx.gridConfig());

        if (keyValFilterBytes != null && keyValFilter == null)
            keyValFilter = U.unmarshal(mrsh, keyValFilterBytes, clsLdr);

        if (rdcBytes != null && rdc == null)
            rdc = U.unmarshal(mrsh, rdcBytes, ldr);

        if (transBytes != null && trans == null)
            trans = U.unmarshal(mrsh, transBytes, clsLdr);

        if (argsBytes != null && args == null)
            args = U.unmarshal(mrsh, argsBytes, clsLdr);

        if (idxQryDescBytes != null && idxQryDesc == null)
            idxQryDesc = U.unmarshal(mrsh, idxQryDescBytes, clsLdr);

        if (!F.isEmpty(skipKeys)) {
            CacheObjectContext objCtx = ctx.cacheObjectContext(cacheId);

            for (KeyCacheObject k : skipKeys)
                k.finishUnmarshal(objCtx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    void beforeLocalExecution(GridCacheContext ctx) throws IgniteCheckedException {
        Marshaller marsh = ctx.marshaller();

        rdc = rdc != null ? U.<IgniteReducer<Object, Object>>unmarshal(marsh, U.marshal(marsh, rdc),
            U.resolveClassLoader(ctx.gridConfig())) : null;
        trans = trans != null ? U.<IgniteClosure<Object, Object>>unmarshal(marsh, U.marshal(marsh, trans),
            U.resolveClassLoader(ctx.gridConfig())) : null;
        idxQryDesc = idxQryDesc != null ? U.<IndexQueryDesc>unmarshal(marsh, U.marshal(marsh, idxQryDesc),
            U.resolveClassLoader(ctx.gridConfig())) : null;
    }

    /**
     * @return Request id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType type() {
        return type;
    }

    /**
     * @return {@code true} if query returns fields.
     */
    public boolean fields() {
        return fields;
    }

    /**
     * @return Query limit.
     */
    public int limit() {
        return limit;
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return clause;
    }

    /**
     * @return Index query description.
     */
    public IndexQueryDesc idxQryDesc() {
        return idxQryDesc;
    }

    /**
     * @return Class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @return Flag indicating whether to include backups.
     */
    public boolean includeBackups() {
        return incBackups;
    }

    /**
     * @return Flag indicating that this is cancel request.
     */
    public boolean cancel() {
        return cancel;
    }

    /**
     * @return Key-value filter.
     */
    public IgniteBiPredicate<Object, Object> keyValueFilter() {
        return keyValFilter;
    }

    /**
     * @return Reducer.
     */
    public IgniteReducer<Object, Object> reducer() {
        return rdc;
    }

    /**
     * @return Transformer.
     */
    public IgniteClosure<?, ?> transformer() {
        return trans;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Include meta data or not.
     */
    public boolean includeMetaData() {
        return incMeta;
    }

    /**
     * @return Whether to load all pages.
     */
    public boolean allPages() {
        return all;
    }

    /**
     * @return Whether to keep binary.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return Task hash.
     */
    public int taskHash() {
        return taskHash;
    }

    /**
     * @return Flag to enable data page scan.
     */
    public Boolean isDataPageScanEnabled() {
        switch (flags & FLAG_DATA_PAGE_SCAN_MASK) {
            case FLAG_DATA_PAGE_SCAN_ENABLED:
                return true;

            case FLAG_DATA_PAGE_SCAN_DISABLED:
                return false;
        }

        return null;
    }

    /** @return Set of keys that must be skiped during iteration. */
    public Set<KeyCacheObject> skipKeys() {
        return skipKeys;
    }

    /**
     * @return partition.
     */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeBoolean("all", all))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("argsBytes", argsBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeString("cacheName", cacheName))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("cancel", cancel))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeString("clause", clause))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeString("clsName", clsName))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("fields", fields))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeLong("id", id))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean("incBackups", incBackups))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeBoolean("incMeta", incMeta))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeBoolean("keepBinary", keepBinary))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeByteArray("keyValFilterBytes", keyValFilterBytes))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeByteArray("rdcBytes", rdcBytes))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeInt("limit", limit))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeInt("taskHash", taskHash))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeByteArray("transBytes", transBytes))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeByteArray("idxQryDescBytes", idxQryDescBytes))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeCollection("skipKeys", skipKeys, MessageCollectionItemType.MSG))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                all = reader.readBoolean("all");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                argsBytes = reader.readByteArray("argsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                cacheName = reader.readString("cacheName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                cancel = reader.readBoolean("cancel");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                clause = reader.readString("clause");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                clsName = reader.readString("clsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                fields = reader.readBoolean("fields");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                incBackups = reader.readBoolean("incBackups");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                incMeta = reader.readBoolean("incMeta");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                keepBinary = reader.readBoolean("keepBinary");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                keyValFilterBytes = reader.readByteArray("keyValFilterBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                rdcBytes = reader.readByteArray("rdcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                limit = reader.readInt("limit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                taskHash = reader.readInt("taskHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                transBytes = reader.readByteArray("transBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = GridCacheQueryType.fromOrdinal(typeOrd);

                reader.incrementState();

            case 25:
                idxQryDescBytes = reader.readByteArray("idxQryDescBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                skipKeys = reader.readCollection("skipKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridCacheQueryRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 58;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this, super.toString());
    }
}
