/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Cache eviction response.
 */
public class GridCacheEvictionResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private long futId;

    /** Rejected keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> rejectedKeys = new HashSet<>();

    /** Serialized rejected keys. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> rejectedKeyBytes;

    /** Flag to indicate whether request processing has finished with error. */
    private boolean err;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     */
    GridCacheEvictionResponse(int cacheId, long futId) {
        this(cacheId, futId, false);
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param err {@code True} if request processing has finished with error.
     */
    GridCacheEvictionResponse(int cacheId, long futId, boolean err) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.err = err;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        rejectedKeyBytes = marshalCollection(rejectedKeys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        rejectedKeys = unmarshalCollection(rejectedKeyBytes, ctx, ldr);
    }

    /**
     * @return Future ID.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Rejected keys.
     */
    Collection<K> rejectedKeys() {
        return rejectedKeys;
    }

    /**
     * Add rejected key to response.
     *
     * @param key Evicted key.
     */
    void addRejected(K key) {
        assert key != null;

        rejectedKeys.add(key);
    }

    /**
     * @return {@code True} if request processing has finished with error.
     */
    boolean error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheEvictionResponse _clone = new GridCacheEvictionResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheEvictionResponse _clone = (GridCacheEvictionResponse)_msg;

        _clone.futId = futId;
        _clone.rejectedKeys = rejectedKeys;
        _clone.rejectedKeyBytes = rejectedKeyBytes;
        _clone.err = err;
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
                if (!commState.putBoolean(null, err))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(null, futId))
                    return false;

                commState.idx++;

            case 5:
                if (rejectedKeyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, rejectedKeyBytes.size()))
                            return false;

                        commState.it = rejectedKeyBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray(null, (byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

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
                if (buf.remaining() < 1)
                    return false;

                err = commState.getBoolean(null);

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                futId = commState.getLong(null);

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (rejectedKeyBytes == null)
                        rejectedKeyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        rejectedKeyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionResponse.class, this);
    }
}
