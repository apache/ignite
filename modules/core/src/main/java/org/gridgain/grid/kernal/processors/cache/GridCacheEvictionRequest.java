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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Cache eviction request.
 */
public class GridCacheEvictionRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future id. */
    private long futId;

    /** Entries to clear from near and backup nodes. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridTuple3<K, GridCacheVersion, Boolean>> entries;

    /** Serialized entries. */
    @GridToStringExclude
    private byte[] entriesBytes;

    /** Topology version. */
    private long topVer;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future id.
     * @param size Size.
     * @param topVer Topology version.
     */
    GridCacheEvictionRequest(int cacheId, long futId, int size, long topVer) {
        assert futId > 0;
        assert size > 0;
        assert topVer > 0;

        this.cacheId = cacheId;
        this.futId = futId;

        entries = new ArrayList<>(size);

        this.topVer = topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (entries != null) {
            if (ctx.deploymentEnabled())
                prepareObjects(entries, ctx);

            entriesBytes = ctx.marshaller().marshal(entries);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (entriesBytes != null)
            entries = ctx.marshaller().unmarshal(entriesBytes, ldr);
    }

    /**
     * @return Future id.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Entries - {{Key, Version, Boolean (near or not)}, ...}.
     */
    Collection<GridTuple3<K, GridCacheVersion, Boolean>> entries() {
        return entries;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * Add key to request.
     *
     * @param key Key to evict.
     * @param ver Entry version.
     * @param near {@code true} if key should be evicted from near cache.
     */
    void addKey(K key, GridCacheVersion ver, boolean near) {
        assert key != null;
        assert ver != null;

        entries.add(F.t(key, ver, near));
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheEvictionRequest _clone = new GridCacheEvictionRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheEvictionRequest _clone = (GridCacheEvictionRequest)_msg;

        _clone.futId = futId;
        _clone.entries = entries;
        _clone.entriesBytes = entriesBytes;
        _clone.topVer = topVer;
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
                if (!commState.putByteArray(entriesBytes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(futId))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putLong(topVer))
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
                byte[] entriesBytes0 = commState.getByteArray();

                if (entriesBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                entriesBytes = entriesBytes0;

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                futId = commState.getLong();

                commState.idx++;

            case 5:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionRequest.class, this);
    }
}
