/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Set data request.
 */
public class GridCacheSetDataRequest<K, V> extends GridCacheMessage<K, V> {
    /** */
    private long id;

    /** */
    private GridUuid setId;

    /** */
    private long topVer;

    /** */
    private int pageSize;

    /** */
    private boolean sizeOnly;

    /** */
    private boolean cancel;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetDataRequest() {
        // No-op.
    }

    /**
     * Creates set data request.
     *
     * @param id Request id.
     * @param setId Set unique ID.
     * @param topVer Topology version.
     * @param pageSize Page size.
     * @param sizeOnly If {@code true} then only set data size is requested.
     */
    public GridCacheSetDataRequest(long id, GridUuid setId, long topVer, int pageSize, boolean sizeOnly) {
        this.id = id;
        this.setId = setId;
        this.topVer = topVer;
        this.pageSize = pageSize;
        this.sizeOnly = sizeOnly;
    }

    /**
     * @return {@code True} if this is cancel request.
     */
    public boolean cancel() {
        return cancel;
    }

    /**
     * @param cancel Cancel flag.
     */
    public void cancel(boolean cancel) {
        this.cancel = cancel;
    }

    /**
     * @return Request id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Set unique ID.
     */
    public GridUuid setId() {
        return setId;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return {@code True} only if set data size is requested.
     */
    public boolean sizeOnly() {
        return sizeOnly;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 79;
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheSetDataRequest _clone = new GridCacheSetDataRequest();

        clone0(_clone);

        return _clone;
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
                if (!commState.putBoolean(cancel))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putLong(id))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putInt(pageSize))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putGridUuid(setId))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putBoolean(sizeOnly))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 2:
                if (buf.remaining() < 1)
                    return false;

                cancel = commState.getBoolean();

                commState.idx++;

            case 3:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

            case 4:
                if (buf.remaining() < 4)
                    return false;

                pageSize = commState.getInt();

                commState.idx++;

            case 5:
                GridUuid setId0 = commState.getGridUuid();

                if (setId0 == GRID_UUID_NOT_READ)
                    return false;

                setId = setId0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 1)
                    return false;

                sizeOnly = commState.getBoolean();

                commState.idx++;

            case 7:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheSetDataRequest _clone = (GridCacheSetDataRequest)_msg;

        _clone.id = id;
        _clone.setId = setId;
        _clone.topVer = topVer;
        _clone.pageSize = pageSize;
        _clone.sizeOnly = sizeOnly;
        _clone.cancel = cancel;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetDataRequest.class, this);
    }
}
