/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.query.GridCacheQueryType.*;

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
    private IgnitePredicate<GridCacheEntry<Object, Object>> prjFilter;

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
    @GridDirectVersion(1)
    private boolean keepPortable;

    /** */
    @GridDirectVersion(2)
    private UUID subjId;

    /** */
    @GridDirectVersion(2)
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
        IgnitePredicate<GridCacheEntry<Object, Object>> prjFilter,
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

        IgniteMarshaller mrsh = ctx.marshaller();

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
        IgniteMarshaller marsh = ctx.marshaller();

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
    public IgnitePredicate<GridCacheEntry<Object, Object>> projectionFilter() {
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
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheQueryRequest _clone = new GridCacheQueryRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 3:
                if (!commState.putBoolean("all", all))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putByteArray("argsBytes", argsBytes))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putString("cacheName", cacheName))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putBoolean("cancel", cancel))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putString("clause", clause))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putString("clsName", clsName))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putBoolean("fields", fields))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putLong("id", id))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putBoolean("incBackups", incBackups))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean("incMeta", incMeta))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putByteArray("keyValFilterBytes", keyValFilterBytes))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putInt("pageSize", pageSize))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putByteArray("prjFilterBytes", prjFilterBytes))
                    return false;

                commState.idx++;

            case 16:
                if (!commState.putByteArray("rdcBytes", rdcBytes))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putByteArray("transBytes", transBytes))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putEnum("type", type))
                    return false;

                commState.idx++;

            case 19:
                if (!commState.putBoolean("keepPortable", keepPortable))
                    return false;

                commState.idx++;

            case 20:
                if (!commState.putUuid("subjId", subjId))
                    return false;

                commState.idx++;

            case 21:
                if (!commState.putInt("taskHash", taskHash))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 3:
                all = commState.getBoolean("all");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 4:
                argsBytes = commState.getByteArray("argsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 5:
                cacheName = commState.getString("cacheName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                cancel = commState.getBoolean("cancel");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 7:
                clause = commState.getString("clause");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 8:
                clsName = commState.getString("clsName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 9:
                fields = commState.getBoolean("fields");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 10:
                id = commState.getLong("id");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 11:
                incBackups = commState.getBoolean("incBackups");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 12:
                incMeta = commState.getBoolean("incMeta");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 13:
                keyValFilterBytes = commState.getByteArray("keyValFilterBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 14:
                pageSize = commState.getInt("pageSize");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 15:
                prjFilterBytes = commState.getByteArray("prjFilterBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 16:
                rdcBytes = commState.getByteArray("rdcBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 17:
                transBytes = commState.getByteArray("transBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 18:
                byte type0 = commState.getByte("type");

                if (!commState.lastRead())
                    return false;

                type = GridCacheQueryType.fromOrdinal(type0);

                commState.idx++;

            case 19:
                keepPortable = commState.getBoolean("keepPortable");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 20:
                subjId = commState.getUuid("subjId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 21:
                taskHash = commState.getInt("taskHash");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 57;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this, super.toString());
    }
}
