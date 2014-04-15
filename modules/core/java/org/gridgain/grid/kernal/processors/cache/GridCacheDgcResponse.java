/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.nio.*;
import java.util.*;

/**
 * DGC response.
 */
public class GridCacheDgcResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    private static final long serialVersionUID = -5680782994753263770L;
    /** */
    @GridToStringInclude
    @GridDirectTransient
    private Map<K, Collection<GridCacheDgcBadLock>> map = new HashMap<>();

    /** */
    @GridToStringExclude
    private byte[] mapBytes;

    /** */
    private boolean rmvLocks;

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (map != null) {
            if (ctx.deploymentEnabled()) {
                for (K key : map.keySet())
                    prepareObject(key, ctx);
            }

            mapBytes = CU.marshal(ctx, map);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (mapBytes != null)
            map = ctx.marshaller().unmarshal(mapBytes, ldr);
    }

    /**
     * Add information about key, tx result and version to response.
     *
     * @param key Key.
     * @param lock Lock.
     */
    void addCandidate(K key, GridCacheDgcBadLock lock) {
        Collection<GridCacheDgcBadLock> col = F.addIfAbsent(map, key, new ArrayList<GridCacheDgcBadLock>());

        assert col != null;

        col.add(lock);
    }

    /**
     * @return Candidates map.
     */
    Map<K, Collection<GridCacheDgcBadLock>> candidatesMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * @return Remove locks flag for this DGC iteration.
     */
    public boolean removeLocks() {
        return rmvLocks;
    }

    /**
     * @param rmvLocks Remove locks flag for this DGC iteration.
     */
    public void removeLocks(boolean rmvLocks) {
        this.rmvLocks = rmvLocks;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheDgcResponse _clone = new GridCacheDgcResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheDgcResponse _clone = (GridCacheDgcResponse)_msg;

        _clone.map = map;
        _clone.mapBytes = mapBytes;
        _clone.rmvLocks = rmvLocks;
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
            case 2:
                if (!commState.putByteArray(mapBytes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putBoolean(rmvLocks))
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
            case 2:
                byte[] mapBytes0 = commState.getByteArray();

                if (mapBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                mapBytes = mapBytes0;

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                rmvLocks = commState.getBoolean();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcResponse.class, this);
    }
}
