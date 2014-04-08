/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Set data response.
 */
public class GridCacheSetDataResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private long id;

    /** */
    private boolean last;

    /** */
    private int size;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> dataBytes;

    /** */
    @GridDirectTransient
    @GridToStringInclude
    private Collection<Object> data;

    /** */
    @GridDirectTransient
    private UUID nodeId;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetDataResponse() {
        // No-op.
    }

    /**
     * @param id Request ID.
     * @param data Set items.
     * @param last {@code True} if this is last response for request.
     */
    public GridCacheSetDataResponse(long id, Collection<Object> data, boolean last) {
        this.id = id;
        this.data = data;
        this.last = last;
    }

    /**
     * @param id Request ID.
     * @param size Set size.
     */
    public GridCacheSetDataResponse(long id, int size) {
        this.id = id;
        this.size = size;
    }

    /**
     * @return ID of the node sent response.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId ID of the node sent response.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Set items.
     */
    public Collection<Object> data() {
        return data;
    }

    /**
     * @return Request ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return {@code True} if this is last response for request.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Set size.
     */
    public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        dataBytes = marshalCollection(data, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        data = unmarshalCollection(dataBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 82;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheSetDataResponse _clone = new GridCacheSetDataResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 2:
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

            case 3:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

            case 4:
                if (buf.remaining() < 1)
                    return false;

                last = commState.getBoolean();

                commState.idx++;

            case 5:
                if (buf.remaining() < 4)
                    return false;

                size = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
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
            case 2:
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

            case 3:
                if (!commState.putLong(id))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putBoolean(last))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putInt(size))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheSetDataResponse _clone = (GridCacheSetDataResponse)_msg;

        _clone.id = id;
        _clone.last = last;
        _clone.size = size;
        _clone.dataBytes = dataBytes;
        _clone.data = data;
        _clone.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetDataResponse.class, this);
    }
}
