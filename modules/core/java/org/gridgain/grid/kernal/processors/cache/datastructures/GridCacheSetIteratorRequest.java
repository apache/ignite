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

import java.nio.*;

/**
 * TODO
 */
public class GridCacheSetIteratorRequest<K, V> extends GridCacheMessage<K, V> {
    /** */
    private long id;

    /** */
    private GridUuid setId;

    /** */
    private long topVer;

    /** */
    private int pageSize;

    public GridCacheSetIteratorRequest() {
        // No-op.
    }

    public GridCacheSetIteratorRequest(long id, GridUuid setId, long topVer, int pageSize) {
        this.id = id;
        this.setId = setId;
        this.topVer = topVer;
        this.pageSize = pageSize;
    }

    @Override public long topologyVersion() {
        return topVer;
    }

    public long id() {
        return id;
    }

    public GridUuid setId() {
        return setId;
    }

    public int pageSize() {
        return pageSize;
    }

    @Override public byte directType() {
        return 79;
    }

    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridCacheSetIteratorRequest<K, V> clone() {
        GridCacheSetIteratorRequest _clone = new GridCacheSetIteratorRequest();

        clone0(_clone);

        return _clone;
    }

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
                if (!commState.putLong(id))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putInt(pageSize))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putGridUuid(setId))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 2:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

            case 3:
                if (buf.remaining() < 4)
                    return false;

                pageSize = commState.getInt();

                commState.idx++;

            case 4:
                GridUuid setId0 = commState.getGridUuid();

                if (setId0 == GRID_UUID_NOT_READ)
                    return false;

                setId = setId0;

                commState.idx++;

            case 5:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheSetIteratorRequest _clone = (GridCacheSetIteratorRequest)_msg;

        _clone.id = id;
        _clone.setId = setId;
        _clone.topVer = topVer;
        _clone.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetIteratorRequest.class, this);
    }
}
