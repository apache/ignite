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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;

import javax.cache.*;
import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.*;

/**
 * Query request.
 */
public class GridCacheQueryRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
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
    private IgnitePredicate<Cache.Entry<Object, Object>> prjFilter;

    /** */
    private byte[] prjFilterBytes;

    /** */
    @GridDirectTransient
    private IgniteReducer<Object, Object> rdc;

    /** */
    private byte[] rdcBytes;

    /** */
    @GridDirectTransient
    private IgniteClosure<Object, Object> trans;

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

    /**
     * Required by {@link Externalizable}
     */
    public GridCacheQueryRequest() {
        // No-op.
    }

    /**
     * @param id Request to cancel.
     * @param fields Fields query flag.
     */
    public GridCacheQueryRequest(int cacheId, long id, boolean fields) {
        this.cacheId = cacheId;
        this.id = id;
        this.fields = fields;

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
     * @param keepPortable Whether to keep portables.
     */
    public GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        int pageSize,
        boolean incBackups,
        boolean fields,
        boolean all,
        boolean keepPortable,
        UUID subjId,
        int taskHash
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.fields = fields;
        this.all = all;
        this.keepPortable = keepPortable;
        this.subjId = subjId;
        this.taskHash = taskHash;
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
     * @param prjFilter Projection filter.
     * @param rdc Reducer.
     * @param trans Transformer.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param args Query arguments.
     * @param incMeta Include meta data or not.
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
        IgnitePredicate<Cache.Entry<Object, Object>> prjFilter,
        IgniteReducer<Object, Object> rdc,
        IgniteClosure<Object, Object> trans,
        int pageSize,
        boolean incBackups,
        Object[] args,
        boolean incMeta,
        boolean keepPortable,
        UUID subjId,
        int taskHash
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
        this.prjFilter = prjFilter;
        this.rdc = rdc;
        this.trans = trans;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.args = args;
        this.incMeta = incMeta;
        this.keepPortable = keepPortable;
        this.subjId = subjId;
        this.taskHash = taskHash;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (keyValFilter != null) {
            if (ctx.deploymentEnabled())
                prepareObject(keyValFilter, ctx);

            keyValFilterBytes = CU.marshal(ctx, keyValFilter);
        }

        if (prjFilter != null) {
            if (ctx.deploymentEnabled())
                prepareObject(prjFilter, ctx);

            prjFilterBytes = CU.marshal(ctx, prjFilter);
        }

        if (rdc != null) {
            if (ctx.deploymentEnabled())
                prepareObject(rdc, ctx);

            rdcBytes = CU.marshal(ctx, rdc);
        }

        if (trans != null) {
            if (ctx.deploymentEnabled())
                prepareObject(trans, ctx);

            transBytes = CU.marshal(ctx, trans);
        }

        if (!F.isEmpty(args)) {
            if (ctx.deploymentEnabled()) {
                for (Object arg : args)
                    prepareObject(arg, ctx);
            }

            argsBytes = CU.marshal(ctx, args);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        Marshaller mrsh = ctx.marshaller();

        if (keyValFilterBytes != null)
            keyValFilter = mrsh.unmarshal(keyValFilterBytes, ldr);

        if (prjFilterBytes != null)
            prjFilter = mrsh.unmarshal(prjFilterBytes, ldr);

        if (rdcBytes != null)
            rdc = mrsh.unmarshal(rdcBytes, ldr);

        if (transBytes != null)
            trans = mrsh.unmarshal(transBytes, ldr);

        if (argsBytes != null)
            args = mrsh.unmarshal(argsBytes, ldr);
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    void beforeLocalExecution(GridCacheContext<K, V> ctx) throws IgniteCheckedException {
        Marshaller marsh = ctx.marshaller();

        rdc = rdc != null ? marsh.<IgniteReducer<Object, Object>>unmarshal(marsh.marshal(rdc), null) : null;
        trans = trans != null ? marsh.<IgniteClosure<Object, Object>>unmarshal(marsh.marshal(trans), null) : null;
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

    /** {@inheritDoc} */
    public IgnitePredicate<Cache.Entry<Object, Object>> projectionFilter() {
        return prjFilter;
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
    public IgniteClosure<Object, Object> transformer() {
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
     * @return Whether to keep portables.
     */
    public boolean keepPortable() {
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

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheQueryRequest _clone = (GridCacheQueryRequest)_msg;

        _clone.id = id;
        _clone.cacheName = cacheName;
        _clone.type = type;
        _clone.fields = fields;
        _clone.clause = clause;
        _clone.clsName = clsName;
        _clone.keyValFilter = keyValFilter;
        _clone.keyValFilterBytes = keyValFilterBytes;
        _clone.prjFilter = prjFilter;
        _clone.prjFilterBytes = prjFilterBytes;
        _clone.rdc = rdc;
        _clone.rdcBytes = rdcBytes;
        _clone.trans = trans;
        _clone.transBytes = transBytes;
        _clone.args = args;
        _clone.argsBytes = argsBytes;
        _clone.pageSize = pageSize;
        _clone.incBackups = incBackups;
        _clone.cancel = cancel;
        _clone.incMeta = incMeta;
        _clone.all = all;
        _clone.keepPortable = keepPortable;
        _clone.subjId = subjId;
        _clone.taskHash = taskHash;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriteState state) {
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf, state))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 3:
                if (!writer.writeBoolean("all", all))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeByteArray("argsBytes", argsBytes))
                    return false;

                state.increment();

            case 5:
                if (!writer.writeString("cacheName", cacheName))
                    return false;

                state.increment();

            case 6:
                if (!writer.writeBoolean("cancel", cancel))
                    return false;

                state.increment();

            case 7:
                if (!writer.writeString("clause", clause))
                    return false;

                state.increment();

            case 8:
                if (!writer.writeString("clsName", clsName))
                    return false;

                state.increment();

            case 9:
                if (!writer.writeBoolean("fields", fields))
                    return false;

                state.increment();

            case 10:
                if (!writer.writeLong("id", id))
                    return false;

                state.increment();

            case 11:
                if (!writer.writeBoolean("incBackups", incBackups))
                    return false;

                state.increment();

            case 12:
                if (!writer.writeBoolean("incMeta", incMeta))
                    return false;

                state.increment();

            case 13:
                if (!writer.writeBoolean("keepPortable", keepPortable))
                    return false;

                state.increment();

            case 14:
                if (!writer.writeByteArray("keyValFilterBytes", keyValFilterBytes))
                    return false;

                state.increment();

            case 15:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                state.increment();

            case 16:
                if (!writer.writeByteArray("prjFilterBytes", prjFilterBytes))
                    return false;

                state.increment();

            case 17:
                if (!writer.writeByteArray("rdcBytes", rdcBytes))
                    return false;

                state.increment();

            case 18:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                state.increment();

            case 19:
                if (!writer.writeInt("taskHash", taskHash))
                    return false;

                state.increment();

            case 20:
                if (!writer.writeByteArray("transBytes", transBytes))
                    return false;

                state.increment();

            case 21:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                all = reader.readBoolean("all");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                argsBytes = reader.readByteArray("argsBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                cacheName = reader.readString("cacheName");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                cancel = reader.readBoolean("cancel");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                clause = reader.readString("clause");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                clsName = reader.readString("clsName");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                fields = reader.readBoolean("fields");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                incBackups = reader.readBoolean("incBackups");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                incMeta = reader.readBoolean("incMeta");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                keepPortable = reader.readBoolean("keepPortable");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                keyValFilterBytes = reader.readByteArray("keyValFilterBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                prjFilterBytes = reader.readByteArray("prjFilterBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                rdcBytes = reader.readByteArray("rdcBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 18:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                taskHash = reader.readInt("taskHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                transBytes = reader.readByteArray("transBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 21:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = GridCacheQueryType.fromOrdinal(typeOrd);

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 58;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this, super.toString());
    }
}
