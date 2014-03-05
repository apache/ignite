/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Lite dht cache backup update request.
 */
public class GridDhtAtomicUpdateRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID. */
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Write version. */
    private GridCacheVersion writeVer;

    /** Topology version. */
    private long topVer;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> keys;

    /** Key bytes. */
    @GridToStringInclude
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Values to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<V> vals;

    /** Value bytes. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> valBytes;

    /** DR versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> drVers;

    /** DR TTLs. */
    private GridLongList drTtls;

    /** DR TTLs. */
    private GridLongList drExpireTimes;

    /** Write synchronization mode. */
    private GridCacheWriteSynchronizationMode syncMode;

    /** Time to live. */
    private long ttl;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param writeVer Write version for cache values.
     * @param syncMode Cache write synchronization mode.
     * @param topVer Topology version.
     * @param ttl Time to live.
     */
    public GridDhtAtomicUpdateRequest(
        UUID nodeId,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        GridCacheWriteSynchronizationMode syncMode,
        long topVer,
        long ttl
    ) {
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.ttl = ttl;
        this.topVer = topVer;

        keys = new ArrayList<>();
        keyBytes = new ArrayList<>();
        vals = new ArrayList<>();
        valBytes = new ArrayList<>();
    }

    /**
     * @param key Key to add.
     * @param keyBytes Key bytes, if key was already serialized.
     * @param val Value, {@code null} if should be removed.
     * @param valBytes Value bytes, {@code null} if should be removed.
     * @param drTtl DR TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     */
    public void addWriteValue(K key, @Nullable byte[] keyBytes, @Nullable V val, @Nullable byte[] valBytes, long drTtl,
        long drExpireTime, @Nullable GridCacheVersion drVer) {
        keys.add(key);
        this.keyBytes.add(keyBytes);
        vals.add(val);
        this.valBytes.add(valBytes != null ? GridCacheValueBytes.marshaled(valBytes) : null);

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

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Keys size.
     */
    public int size() {
        return keys.size();
    }

    /**
     * @return Version assigned on primary node.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public GridCacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Keys.
     */
    public Collection<K> keys() {
        return keys;
    }

    /**
     * @param idx Key index.
     * @return Key.
     */
    public K key(int idx) {
        return keys.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Key bytes.
     */
    public byte[] keyBytes(int idx) {
        return keyBytes == null ? null : keyBytes.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public V value(int idx) {
        if (vals != null) {
            V val = vals.get(idx);

            if (val != null)
                return val;
        }

        if (valBytes != null) {
            GridCacheValueBytes valBytes0 = valBytes.get(idx);

            if (valBytes0 != null && valBytes0.isPlain())
                return (V)valBytes0.get();
        }

        return null;
    }

    /**
     * @param idx Key index.
     * @return Value bytes.
     */
    @Nullable public byte[] valueBytes(int idx) {
        if (valBytes != null) {
            GridCacheValueBytes valBytes0 = valBytes.get(idx);

            if (valBytes0 != null && !valBytes0.isPlain())
                return valBytes0.get();
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

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        keyBytes = marshalCollection(keys, ctx);
        valBytes = marshalValuesCollection(vals, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        keys = unmarshalCollection(keyBytes, ctx, ldr);
        vals = unmarshalValueBytesCollection(valBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtAtomicUpdateRequest _clone = new GridDhtAtomicUpdateRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAtomicUpdateRequest _clone = (GridDhtAtomicUpdateRequest)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer;
        _clone.writeVer = writeVer;
        _clone.topVer = topVer;
        _clone.keys = keys;
        _clone.keyBytes = keyBytes;
        _clone.vals = vals;
        _clone.valBytes = valBytes;
        _clone.drVers = drVers;
        _clone.drTtls = drTtls;
        _clone.drExpireTimes = drExpireTimes;
        _clone.syncMode = syncMode;
        _clone.ttl = ttl;
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
                if (!commState.putLongList(drExpireTimes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putLongList(drTtls))
                    return false;

                commState.idx++;

            case 4:
                if (drVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(drVers.size()))
                            return false;

                        commState.it = drVers.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putCacheVersion((GridCacheVersion)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 5:
                if (!commState.putCacheVersion(futVer))
                    return false;

                commState.idx++;

            case 6:
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

            case 7:
                if (!commState.putUuid(nodeId))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putEnum(syncMode))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putLong(ttl))
                    return false;

                commState.idx++;

            case 11:
                if (valBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(valBytes.size()))
                            return false;

                        commState.it = valBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putValueBytes((GridCacheValueBytes)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 12:
                if (!commState.putCacheVersion(writeVer))
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
                GridLongList drExpireTimes0 = commState.getLongList();

                if (drExpireTimes0 == LONG_LIST_NOT_READ)
                    return false;

                drExpireTimes = drExpireTimes0;

                commState.idx++;

            case 3:
                GridLongList drTtls0 = commState.getLongList();

                if (drTtls0 == LONG_LIST_NOT_READ)
                    return false;

                drTtls = drTtls0;

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (drVers == null)
                        drVers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        drVers.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 5:
                GridCacheVersion futVer0 = commState.getCacheVersion();

                if (futVer0 == CACHE_VER_NOT_READ)
                    return false;

                futVer = futVer0;

                commState.idx++;

            case 6:
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

            case 7:
                UUID nodeId0 = commState.getUuid();

                if (nodeId0 == UUID_NOT_READ)
                    return false;

                nodeId = nodeId0;

                commState.idx++;

            case 8:
                Object syncMode0 = commState.getEnum(GridCacheWriteSynchronizationMode.class);

                if (syncMode0 == ENUM_NOT_READ)
                    return false;

                syncMode = (GridCacheWriteSynchronizationMode)syncMode0;

                commState.idx++;

            case 9:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 10:
                if (buf.remaining() < 8)
                    return false;

                ttl = commState.getLong();

                commState.idx++;

            case 11:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (valBytes == null)
                        valBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheValueBytes _val = commState.getValueBytes();

                        if (_val == VAL_BYTES_NOT_READ)
                            return false;

                        valBytes.add((GridCacheValueBytes)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 12:
                GridCacheVersion writeVer0 = commState.getCacheVersion();

                if (writeVer0 == CACHE_VER_NOT_READ)
                    return false;

                writeVer = writeVer0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 37;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateRequest.class, this);
    }
}
