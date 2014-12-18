/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicUpdateRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Fast map flag. */
    private boolean fastMap;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private long topVer;

    /** Write synchronization mode. */
    private GridCacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Keys to update. */
    @GridDirectTransient
    @GridToStringInclude
    private List<K> keys;

    /** Key bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Values to update. */
    @GridDirectTransient
    private List<Object> vals;

    /** Value bytes. */
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> valBytes;

    /** DR versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> drVers;

    /** DR TTLs. */
    private GridLongList drTtls;

    /** DR TTLs. */
    private GridLongList drExpireTimes;

    /** Return value flag. */
    private boolean retval;

    /** Time to live. */
    private long ttl;

    /** Filter. */
    @GridDirectTransient
    private IgnitePredicate<GridCacheEntry<K, V>>[] filter;

    /** Filter bytes. */
    private byte[][] filterBytes;

    /** Flag indicating whether request contains primary keys. */
    private boolean hasPrimary;

    /** Force transform backups flag. */
    @GridDirectVersion(2)
    private boolean forceTransformBackups;

    /** Subject ID. */
    @GridDirectVersion(3)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(4)
    private int taskNameHash;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param ttl Time to live.
     * @param filter Optional filter for atomic check.
     */
    public GridNearAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        long topVer,
        GridCacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        boolean forceTransformBackups,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.fastMap = fastMap;
        this.updateVer = updateVer;

        this.topVer = topVer;
        this.syncMode = syncMode;
        this.op = op;
        this.retval = retval;
        this.forceTransformBackups = forceTransformBackups;
        this.ttl = ttl;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        keys = new ArrayList<>();
        vals = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Mapped node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public boolean fastMap() {
        return fastMap;
    }

    /**
     * @return Update version for fast-map request.
     */
    public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public GridCacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Return value flag.
     */
    public boolean returnValue() {
        return retval;
    }

    /**
     * @return Filter.
     */
    @Nullable public IgnitePredicate<GridCacheEntry<K, V>>[] filter() {
        return filter;
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param drTtl DR TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(K key, @Nullable Object val, long drTtl, long drExpireTime,
        @Nullable GridCacheVersion drVer, boolean primary) {
        assert val != null || op == DELETE;
        assert op != TRANSFORM || val instanceof IgniteClosure;

        keys.add(key);
        vals.add(val);

        hasPrimary |= primary;

        // In case there is no DR, do not create the list.
        if (drVer != null) {
            if (drVers == null) {
                drVers = new ArrayList<>();

                for (int i = 0; i < keys.size() - 1; i++)
                    drVers.add(null);
            }

            drVers.add(drVer);
        }
        else if (drVers != null)
            drVers.add(drVer);

        if (drTtl >= 0) {
            if (drTtls == null) {
                drTtls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    drTtls.add(-1);
            }

            drTtls.add(drTtl);
        }

        if (drExpireTime >= 0) {
            if (drExpireTimes == null) {
                drExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    drExpireTimes.add(-1);
            }

            drExpireTimes.add(drExpireTime);
        }
    }

    /**
     * @return Keys for this update request.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @return Values for this update request.
     */
    public List<Object> values() {
        return vals;
    }

    /**
     * @return Update operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    public V value(int idx) {
        assert op == UPDATE : op;

        return (V)vals.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    public IgniteClosure<V, V> transformClosure(int idx) {
        assert op == TRANSFORM : op;

        return (IgniteClosure<V, V>)vals.get(idx);
    }

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public Object writeValue(int idx) {
        if (vals != null) {
            Object val = vals.get(idx);

            if (val != null)
                return val;
        }

        if (valBytes != null) {
            GridCacheValueBytes valBytesTuple = valBytes.get(idx);

            if (valBytesTuple != null && valBytesTuple.isPlain())
                return valBytesTuple.get();
        }

        return null;
    }

    /**
     * @param idx Key index.
     * @return Value bytes.
     */
    public byte[] valueBytes(int idx) {
        if (op != TRANSFORM && valBytes != null) {
            GridCacheValueBytes valBytesTuple = valBytes.get(idx);

            if (valBytesTuple != null && !valBytesTuple.isPlain())
                return valBytesTuple.get();
        }

        return null;
    }

    /**
     * @return DR versions.
     */
    @Nullable public List<GridCacheVersion> drVersions() {
        return drVers;
    }

    /**
     * @param idx Index.
     * @return DR version.
     */
    @Nullable public GridCacheVersion drVersion(int idx) {
        if (drVers != null) {
            assert idx >= 0 && idx < drVers.size();

            return drVers.get(idx);
        }

        return null;
    }

    /**
     * @return DR TTLs.
     */
    @Nullable public GridLongList drTtls() {
        return drTtls;
    }

    /**
     * @param idx Index.
     * @return DR TTL.
     */
    public long drTtl(int idx) {
        if (drTtls != null) {
            assert idx >= 0 && idx < drTtls.size();

            return drTtls.get(idx);
        }

        return -1L;
    }

    /**
     * @return DR TTLs.
     */
    @Nullable public GridLongList drExpireTimes() {
        return drExpireTimes;
    }

    /**
     * @param idx Index.
     * @return DR TTL.
     */
    public long drExpireTime(int idx) {
        if (drExpireTimes != null) {
            assert idx >= 0 && idx < drExpireTimes.size();

            return drExpireTimes.get(idx);
        }

        return -1L;
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    public boolean hasPrimary() {
        return hasPrimary;
    }

    /**
     * @return Force transform backups flag.
     */
    public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /**
     * @param forceTransformBackups Force transform backups flag.
     */
    public void forceTransformBackups(boolean forceTransformBackups) {
        this.forceTransformBackups = forceTransformBackups;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        keyBytes = marshalCollection(keys, ctx);
        valBytes = marshalValuesCollection(vals, ctx);
        filterBytes = marshalFilter(filter, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        keys = unmarshalCollection(keyBytes, ctx, ldr);
        vals = unmarshalValueBytesCollection(valBytes, ctx, ldr);
        filter = unmarshalFilter(filterBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearAtomicUpdateRequest _clone = new GridNearAtomicUpdateRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearAtomicUpdateRequest _clone = (GridNearAtomicUpdateRequest)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer;
        _clone.fastMap = fastMap;
        _clone.updateVer = updateVer;
        _clone.topVer = topVer;
        _clone.syncMode = syncMode;
        _clone.op = op;
        _clone.keys = keys;
        _clone.keyBytes = keyBytes;
        _clone.vals = vals;
        _clone.valBytes = valBytes;
        _clone.drVers = drVers;
        _clone.drTtls = drTtls;
        _clone.drExpireTimes = drExpireTimes;
        _clone.retval = retval;
        _clone.ttl = ttl;
        _clone.filter = filter;
        _clone.filterBytes = filterBytes;
        _clone.hasPrimary = hasPrimary;
        _clone.forceTransformBackups = forceTransformBackups;
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
                if (!commState.putLongList(null, drExpireTimes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLongList(null, drTtls))
                    return false;

                commState.idx++;

            case 5:
                if (drVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, drVers.size()))
                            return false;

                        commState.it = drVers.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putCacheVersion(null, (GridCacheVersion)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 6:
                if (!commState.putBoolean(null, fastMap))
                    return false;

                commState.idx++;

            case 7:
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

            case 8:
                if (!commState.putCacheVersion(null, futVer))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putBoolean(null, hasPrimary))
                    return false;

                commState.idx++;

            case 10:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, keyBytes.size()))
                            return false;

                        commState.it = keyBytes.iterator();
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

            case 11:
                if (!commState.putEnum(null, op))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean(null, retval))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putEnum(null, syncMode))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putLong(null, topVer))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putLong(null, ttl))
                    return false;

                commState.idx++;

            case 16:
                if (!commState.putCacheVersion(null, updateVer))
                    return false;

                commState.idx++;

            case 17:
                if (valBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, valBytes.size()))
                            return false;

                        commState.it = valBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putValueBytes(null, (GridCacheValueBytes)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 18:
                if (!commState.putBoolean(null, forceTransformBackups))
                    return false;

                commState.idx++;

            case 19:
                if (!commState.putUuid(null, subjId))
                    return false;

                commState.idx++;

            case 20:
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
                GridLongList drExpireTimes0 = commState.getLongList(null);

                if (drExpireTimes0 == LONG_LIST_NOT_READ)
                    return false;

                drExpireTimes = drExpireTimes0;

                commState.idx++;

            case 4:
                GridLongList drTtls0 = commState.getLongList(null);

                if (drTtls0 == LONG_LIST_NOT_READ)
                    return false;

                drTtls = drTtls0;

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (drVers == null)
                        drVers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion(null);

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        drVers.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 1)
                    return false;

                fastMap = commState.getBoolean(null);

                commState.idx++;

            case 7:
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

            case 8:
                GridCacheVersion futVer0 = commState.getCacheVersion(null);

                if (futVer0 == CACHE_VER_NOT_READ)
                    return false;

                futVer = futVer0;

                commState.idx++;

            case 9:
                if (buf.remaining() < 1)
                    return false;

                hasPrimary = commState.getBoolean(null);

                commState.idx++;

            case 10:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        keyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 1)
                    return false;

                byte op0 = commState.getByte(null);

                op = GridCacheOperation.fromOrdinal(op0);

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                retval = commState.getBoolean(null);

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                byte syncMode0 = commState.getByte(null);

                syncMode = GridCacheWriteSynchronizationMode.fromOrdinal(syncMode0);

                commState.idx++;

            case 14:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong(null);

                commState.idx++;

            case 15:
                if (buf.remaining() < 8)
                    return false;

                ttl = commState.getLong(null);

                commState.idx++;

            case 16:
                GridCacheVersion updateVer0 = commState.getCacheVersion(null);

                if (updateVer0 == CACHE_VER_NOT_READ)
                    return false;

                updateVer = updateVer0;

                commState.idx++;

            case 17:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt(null);
                }

                if (commState.readSize >= 0) {
                    if (valBytes == null)
                        valBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheValueBytes _val = commState.getValueBytes(null);

                        if (_val == VAL_BYTES_NOT_READ)
                            return false;

                        valBytes.add((GridCacheValueBytes)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 18:
                if (buf.remaining() < 1)
                    return false;

                forceTransformBackups = commState.getBoolean(null);

                commState.idx++;

            case 19:
                UUID subjId0 = commState.getUuid(null);

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 20:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 39;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
