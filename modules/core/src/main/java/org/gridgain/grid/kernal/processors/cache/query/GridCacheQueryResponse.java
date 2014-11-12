/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Query request.
 */
public class GridCacheQueryResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean finished;

    /** */
    private long reqId;

    /** */
    @GridDirectTransient
    private Throwable err;

    /** */
    private byte[] errBytes;

    /** */
    private boolean fields;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> metaDataBytes;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private List<GridIndexingFieldMetadata> metadata;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> dataBytes;

    /** */
    @GridDirectTransient
    private Collection<Object> data;

    /**
     * Empty constructor for {@link Externalizable}
     */
    public GridCacheQueryResponse() {
        //No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param finished Last response or not.
     * @param fields Fields query or not.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, boolean finished, boolean fields) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.finished = finished;
        this.fields = fields;
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param err Error.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, Throwable err) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.err = err;
        finished = true;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);

        metaDataBytes = marshalCollection(metadata, ctx);
        dataBytes = fields ? marshalFieldsCollection(data, ctx) : marshalCollection(data, ctx);

        if (ctx.deploymentEnabled() && !F.isEmpty(data)) {
            for (Object o : data) {
                if (o instanceof Map.Entry) {
                    Map.Entry e = (Map.Entry)o;

                    prepareObject(e.getKey(), ctx);
                    prepareObject(e.getValue(), ctx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);

        metadata = unmarshalCollection(metaDataBytes, ctx, ldr);
        data = fields ? unmarshalFieldsCollection(dataBytes, ctx, ldr) : unmarshalCollection(dataBytes, ctx, ldr);
    }

    /**
     * @return Metadata.
     */
    public List<GridIndexingFieldMetadata> metadata() {
        return metadata;
    }

    /**
     * @param metadata Metadata.
     */
    public void metadata(@Nullable List<GridIndexingFieldMetadata> metadata) {
        this.metadata = metadata;
    }

    /**
     * @return Query data.
     */
    public Collection<Object> data() {
        return data;
    }

    /**
     * @param data Query data.
     */
    @SuppressWarnings("unchecked")
    public void data(Collection<?> data) {
        this.data = (Collection<Object>)data;
    }

    /**
     * @return If this is last response for this request or not.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @param finished If this is last response for this request or not.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @return If fields query.
     */
    public boolean fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TypeMayBeWeakened")
    @Nullable private Collection<byte[]> marshalFieldsCollection(@Nullable Collection<Object> col,
        GridCacheSharedContext<K, V> ctx) throws GridException {
        assert ctx != null;

        if (col == null)
            return null;

        Collection<List<Object>> col0 = new ArrayList<>(col.size());

        for (Object o : col) {
            List<GridIndexingEntity<?>> list = (List<GridIndexingEntity<?>>)o;
            List<Object> list0 = new ArrayList<>(list.size());

            for (GridIndexingEntity<?> ent : list) {
                if (ent.bytes() != null)
                    list0.add(ent.bytes());
                else {
                    if (ctx.deploymentEnabled())
                        prepareObject(ent.value(), ctx);

                    list0.add(CU.marshal(ctx, ent.value()));
                }
            }

            col0.add(list0);
        }

        return marshalCollection(col0, ctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TypeMayBeWeakened")
    @Nullable private Collection<Object> unmarshalFieldsCollection(@Nullable Collection<byte[]> byteCol,
        GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        assert ctx != null;
        assert ldr != null;

        Collection<Object> col = unmarshalCollection(byteCol, ctx, ldr);
        Collection<Object> col0 = null;

        if (col != null) {
            col0 = new ArrayList<>(col.size());

            for (Object o : col) {
                List<Object> list = (List<Object>)o;
                List<Object> list0 = new ArrayList<>(list.size());

                for (Object obj : list)
                    list0.add(obj != null ? ctx.marshaller().unmarshal((byte[])obj, ldr) : null);

                col0.add(list0);
            }
        }

        return col0;
    }

    /**
     * @param out Object output.
     * @throws IOException If failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void writeFieldsCollection(ObjectOutput out) throws IOException {
        assert fields;

        out.writeInt(data != null ? data.size() : -1);

        if (data == null)
            return;

        for (Object o : data) {
            List<GridIndexingEntity<?>> list = (List<GridIndexingEntity<?>>)o;

            out.writeInt(list.size());

            for (GridIndexingEntity<?> idxEnt : list) {
                try {
                    out.writeObject(idxEnt.value());
                }
                catch (GridSpiException e) {
                    throw new IOException("Failed to write indexing entity: " + idxEnt, e);
                }
            }
        }
    }

    /**
     * @param in Object input.
     * @return Read collection.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If failed.
     */
    private Collection<Object> readFieldsCollection(ObjectInput in) throws IOException, ClassNotFoundException {
        assert fields;

        int size = in.readInt();

        if (size == -1)
            return null;

        Collection<Object> res = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            int size0 = in.readInt();

            Collection<Object> col = new ArrayList<>(size0);

            for (int j = 0; j < size0; j++)
                col.add(in.readObject());

            assert col.size() == size0;

            res.add(col);
        }

        assert res.size() == size;

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheQueryResponse _clone = new GridCacheQueryResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheQueryResponse _clone = (GridCacheQueryResponse)_msg;

        _clone.finished = finished;
        _clone.reqId = reqId;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.fields = fields;
        _clone.metaDataBytes = metaDataBytes;
        _clone.metadata = metadata;
        _clone.dataBytes = dataBytes;
        _clone.data = data;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 3:
                if (dataBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(dataBytes.size()))
                            return false;

                        commState.it = dataBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 4:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putBoolean(fields))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putBoolean(finished))
                    return false;

                commState.idx++;

            case 7:
                if (metaDataBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(metaDataBytes.size()))
                            return false;

                        commState.it = metaDataBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 8:
                if (!commState.putLong(reqId))
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
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (dataBytes == null)
                        dataBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        dataBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 4:
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 5:
                if (buf.remaining() < 1)
                    return false;

                fields = commState.getBoolean();

                commState.idx++;

            case 6:
                if (buf.remaining() < 1)
                    return false;

                finished = commState.getBoolean();

                commState.idx++;

            case 7:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (metaDataBytes == null)
                        metaDataBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        metaDataBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 8:
                if (buf.remaining() < 8)
                    return false;

                reqId = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 58;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
