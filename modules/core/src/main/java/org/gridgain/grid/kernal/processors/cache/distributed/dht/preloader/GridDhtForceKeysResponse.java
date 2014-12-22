/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Force keys response. Contains absent keys.
 */
public class GridDhtForceKeysResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini-future ID. */
    private IgniteUuid miniId;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> missedKeyBytes;

    /** Missed (not found) keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> missedKeys;

    /** Cache entries. */
    @GridToStringInclude
    @GridDirectTransient
    private List<GridCacheEntryInfo<K, V>> infos;

    /** */
    private byte[] infosBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtForceKeysResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Request id.
     * @param miniId Mini-future ID.
     */
    public GridDhtForceKeysResponse(int cacheId, IgniteUuid futId, IgniteUuid miniId) {
        assert futId != null;
        assert miniId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Keys.
     */
    public Collection<K> missedKeys() {
        return missedKeys == null ? Collections.<K>emptyList() : missedKeys;
    }

    /**
     * @return Forced entries.
     */
    public Collection<GridCacheEntryInfo<K, V>> forcedInfos() {
        return infos == null ? Collections.<GridCacheEntryInfo<K,V>>emptyList() : infos;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini-future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param key Key.
     */
    public void addMissed(K key) {
        if (missedKeys == null)
            missedKeys = new ArrayList<>();

        missedKeys.add(key);
    }

    /**
     * @param info Entry info to add.
     */
    public void addInfo(GridCacheEntryInfo<K, V> info) {
        assert info != null;

        if (infos == null)
            infos = new ArrayList<>();

        infos.add(info);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (missedKeys != null && missedKeyBytes == null)
            missedKeyBytes = marshalCollection(missedKeys, ctx);

        if (infos != null) {
            marshalInfos(infos, ctx);

            infosBytes = ctx.marshaller().marshal(infos);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (missedKeys == null && missedKeyBytes != null)
            missedKeys = unmarshalCollection(missedKeyBytes, ctx, ldr);

        if (infosBytes != null) {
            infos = ctx.marshaller().unmarshal(infosBytes, ldr);

            unmarshalInfos(infos, ctx.cacheContext(cacheId()), ldr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtForceKeysResponse _clone = new GridDhtForceKeysResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtForceKeysResponse _clone = (GridDhtForceKeysResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.missedKeyBytes = missedKeyBytes;
        _clone.missedKeys = missedKeys;
        _clone.infos = infos;
        _clone.infosBytes = infosBytes;
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
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putByteArray("infosBytes", infosBytes))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 6:
                if (missedKeyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, missedKeyBytes.size()))
                            return false;

                        commState.it = missedKeyBytes.iterator();
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
                IgniteUuid futId0 = commState.getGridUuid("futId");

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 4:
                byte[] infosBytes0 = commState.getByteArray("infosBytes");

                if (infosBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                infosBytes = infosBytes0;

                commState.idx++;

            case 5:
                IgniteUuid miniId0 = commState.getGridUuid("miniId");

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 6:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (missedKeyBytes == null)
                        missedKeyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        missedKeyBytes.add((byte[])_val);

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
        return 42;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysResponse.class, this, super.toString());
    }
}
