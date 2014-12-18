/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Get request.
 */
public class GridNearGetRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Sub ID. */
    private IgniteUuid miniId;

    /** Version. */
    private GridCacheVersion ver;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private LinkedHashMap<K, Boolean> keys;

    /** Reload flag. */
    private boolean reload;

    /** */
    @GridToStringExclude
    @GridDirectMap(keyType = byte[].class, valueType = boolean.class)
    private LinkedHashMap<byte[], Boolean> keyBytes;

    /** Filter bytes. */
    private byte[][] filterBytes;

    /** Topology version. */
    private long topVer;

    /** Filters. */
    @GridDirectTransient
    private IgnitePredicate<GridCacheEntry<K, V>>[] filter;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param keys Keys.
     * @param reload Reload flag.
     * @param topVer Topology version.
     * @param filter Filter.
     */
    public GridNearGetRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        LinkedHashMap<K, Boolean> keys,
        boolean reload,
        long topVer,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        UUID subjId,
        int taskNameHash
    ) {
        assert futId != null;
        assert miniId != null;
        assert ver != null;
        assert keys != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;
        this.keys = keys;
        this.reload = reload;
        this.topVer = topVer;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets task name hash.
     *
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Keys
     */
    public LinkedHashMap<K, Boolean> keys() {
        return keys;
    }

    /**
     * @return Reload flag.
     */
    public boolean reload() {
        return reload;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Filters.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] filter() {
        return filter;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert ctx != null;
        assert !F.isEmpty(keys);

        if (keyBytes == null)
            keyBytes = marshalBooleanLinkedMap(keys, ctx);

        if (filterBytes == null)
            filterBytes = marshalFilter(filter, ctx);
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null)
            keys = unmarshalBooleanLinkedMap(keyBytes, ctx, ldr);

        if (filter == null && filterBytes != null)
            filter = unmarshalFilter(filterBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearGetRequest _clone = new GridNearGetRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearGetRequest _clone = (GridNearGetRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.ver = ver;
        _clone.keys = keys;
        _clone.reload = reload;
        _clone.keyBytes = keyBytes;
        _clone.filterBytes = filterBytes;
        _clone.topVer = topVer;
        _clone.filter = filter;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
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
                if (filterBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, filterBytes.length))
                            return false;

                        commState.it = arrayIterator(filterBytes);
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

            case 4:
                if (!commState.putGridUuid(null, futId))
                    return false;

                commState.idx++;

            case 5:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, keyBytes.size()))
                            return false;

                        commState.it = keyBytes.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<byte[], Boolean> e = (Map.Entry<byte[], Boolean>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putByteArray(null, e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putBoolean(null, e.getValue()))
                            return false;

                        commState.keyDone = false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 6:
                if (!commState.putGridUuid(null, miniId))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putBoolean(null, reload))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putLong(null, topVer))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putCacheVersion(null, ver))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putUuid(null, subjId))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putInt(null, taskNameHash))
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

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (filterBytes == null)
                        filterBytes = new byte[commState.readSize][];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        filterBytes[i] = (byte[])_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 4:
                IgniteUuid futId0 = commState.getGridUuid(null);

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new LinkedHashMap<>(commState.readSize, 1.0f);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            byte[] _val = commState.getByteArray(null);

                            if (_val == BYTE_ARR_NOT_READ)
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        if (buf.remaining() < 1)
                            return false;

                        boolean _val = commState.getBoolean(null);

                        keyBytes.put((byte[])commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 6:
                IgniteUuid miniId0 = commState.getGridUuid(null);

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 7:
                if (buf.remaining() < 1)
                    return false;

                reload = commState.getBoolean(null);

                commState.idx++;

            case 8:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong(null);

                commState.idx++;

            case 9:
                GridCacheVersion ver0 = commState.getCacheVersion(null);

                if (ver0 == CACHE_VER_NOT_READ)
                    return false;

                ver = ver0;

                commState.idx++;

            case 10:
                UUID subjId0 = commState.getUuid(null);

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 48;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetRequest.class, this);
    }
}
