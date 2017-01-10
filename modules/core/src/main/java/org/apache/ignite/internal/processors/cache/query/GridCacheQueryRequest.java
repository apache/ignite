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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SPI;

/**
 * Query request.
 */
public class GridCacheQueryRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

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
    private boolean keepPortable;

    /** */
    private UUID subjId;

    /** */
    private int taskHash;

    /** Partition. */
    private int part = -1;

    /** */
    private AffinityTopologyVersion topVer;

    /**
     * Required by {@link Externalizable}
     */
    public GridCacheQueryRequest() {
        // No-op.
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
    public GridCacheQueryRequest(int cacheId,
        long id,
        boolean fields,
        AffinityTopologyVersion topVer,
        boolean addDepInfo) {
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
     * @param subjId Subject ID.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        int pageSize,
        boolean incBackups,
        boolean fields,
        boolean all,
        boolean keepBinary,
        UUID subjId,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.fields = fields;
        this.all = all;
        this.keepPortable = keepBinary;
        this.subjId = subjId;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param cacheId Cache ID.
     * @param id Request id.
     * @param cacheName Cache name.
     * @param type Query type.
     * @param fields {@code true} if query returns fields.
     * @param clause Query clause.
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
     * @param subjId Subject ID.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        GridCacheQueryType type,
        boolean fields,
        String clause,
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
        UUID subjId,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        assert type != null || fields;
        assert clause != null || (type == SCAN || type == SET || type == SPI);
        assert clsName != null || fields || type == SCAN || type == SET || type == SPI;

        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.type = type;
        this.fields = fields;
        this.clause = clause;
        this.clsName = clsName;
        this.keyValFilter = keyValFilter;
        this.part = part == null ? -1 : part;
        this.rdc = rdc;
        this.trans = trans;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.args = args;
        this.incMeta = incMeta;
        this.keepPortable = keepBinary;
        this.subjId = subjId;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
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
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        Marshaller mrsh = ctx.marshaller();

        if (keyValFilterBytes != null && keyValFilter == null)
            keyValFilter = U.unmarshal(mrsh, keyValFilterBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (rdcBytes != null && rdc == null)
            rdc = U.unmarshal(mrsh, rdcBytes, ldr);

        if (transBytes != null && trans == null)
            trans = U.unmarshal(mrsh, transBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (argsBytes != null && args == null)
            args = U.unmarshal(mrsh, argsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
     * @return Query clause.
     */
    public String clause() {
        return clause;
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
        return keepPortable;
    }

    /**
     * @return Security subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task hash.
     */
    public int taskHash() {
        return taskHash;
    }

    /**
     * @return partition.
     */
    public int partition() {
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
            case 3:
                if (!writer.writeBoolean("all", all))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("argsBytes", argsBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeString("cacheName", cacheName))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeBoolean("cancel", cancel))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeString("clause", clause))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeString("clsName", clsName))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("fields", fields))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeLong("id", id))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("incBackups", incBackups))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("incMeta", incMeta))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean("keepPortable", keepPortable))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByteArray("keyValFilterBytes", keyValFilterBytes))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeByteArray("rdcBytes", rdcBytes))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeInt("taskHash", taskHash))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeByteArray("transBytes", transBytes))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
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
            case 3:
                all = reader.readBoolean("all");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                argsBytes = reader.readByteArray("argsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                cacheName = reader.readString("cacheName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                cancel = reader.readBoolean("cancel");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                clause = reader.readString("clause");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                clsName = reader.readString("clsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                fields = reader.readBoolean("fields");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                incBackups = reader.readBoolean("incBackups");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                incMeta = reader.readBoolean("incMeta");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                keepPortable = reader.readBoolean("keepPortable");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                keyValFilterBytes = reader.readByteArray("keyValFilterBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                rdcBytes = reader.readByteArray("rdcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                taskHash = reader.readInt("taskHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                transBytes = reader.readByteArray("transBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = GridCacheQueryType.fromOrdinal(typeOrd);

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheQueryRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 58;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this, super.toString());
    }
}
