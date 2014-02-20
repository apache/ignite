// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.nio.*;
import java.util.*;

/**
 * Force keys request. This message is sent by node while preloading to force
 * another node to put given keys into the next batch of transmitting entries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedForceKeysRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Future ID. */
    private GridUuid futId;

    /** Serialized keys. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> keyBytes;

    /** Keys to request. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> keys;

    /** Topology version for which keys are requested. */
    private long topVer;

    /**
     * Required by {@link java.io.Externalizable}.
     */
    public GridReplicatedForceKeysRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param keys Keys.
     * @param topVer Topology version for which keys are requested.
     */
    GridReplicatedForceKeysRequest(GridUuid futId, Collection<K> keys, long topVer) {
        assert futId != null;
        assert !F.isEmpty(keys);

        this.futId = futId;
        this.keys = keys;
        this.topVer = topVer;
    }

    /**
     * @return Topology version for which keys are requested.
     */
    public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Future ID.
     */
    public GridUuid futureId() {
        return futId;
    }

    /**
     * @return Collection of serialized keys.
     */
    public Collection<byte[]> keyBytes() {
        return keyBytes;
    }

    /**
     * @return Keys.
     */
    public Collection<K> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (keys != null)
            keyBytes = marshalCollection(keys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (keyBytes != null)
            keys = unmarshalCollection(keyBytes, ctx, ldr);
    }

    /**
     * @return Key count.
     */
    private int keyCount() {
        return keyBytes == null ? keys.size() : keyBytes.size();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridReplicatedForceKeysRequest _clone = new GridReplicatedForceKeysRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridReplicatedForceKeysRequest _clone = (GridReplicatedForceKeysRequest)_msg;

        _clone.futId = futId;
        _clone.keyBytes = keyBytes;
        _clone.keys = keys;
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
            case 2:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 3:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(keyBytes.size()))
                            return false;

                        commState.it = keyBytes.iterator();
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
            case 2:
                GridUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 3:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        keyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 53;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedForceKeysRequest.class, this, "keyCnt", keyCount(), "super", super.toString());
    }
}
