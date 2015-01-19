/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridCacheTtlUpdateRequest<K, V> extends GridCacheMessage<K, V> {
    /** Entries keys. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> keys;

    /** Keys bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keysBytes;

    /** Entries versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> vers;

    /** Near entries keys. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> nearKeys;

    /** Near entries bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearKeysBytes;

    /** Near entries versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> nearVers;

    /** New TTL. */
    private long ttl;

    /** Topology version. */
    private long topVer;

    /**
     * Required empty constructor.
     */
    public GridCacheTtlUpdateRequest() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param ttl TTL.
     */
    public GridCacheTtlUpdateRequest(long topVer, long ttl) {
        assert ttl >= 0 : ttl;

        this.topVer = topVer;
        this.ttl = ttl;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param keyBytes Key bytes.
     * @param ver Version.
     */
    public void addEntry(byte[] keyBytes, GridCacheVersion ver) {
        if (keysBytes == null) {
            keysBytes = new ArrayList<>();

            vers = new ArrayList<>();
        }

        keysBytes.add(keyBytes);

        vers.add(ver);
    }

    /**
     * @param keyBytes Key bytes.
     * @param ver Version.
     */
    public void addNearEntry(byte[] keyBytes, GridCacheVersion ver) {
        if (nearKeysBytes == null) {
            nearKeysBytes = new ArrayList<>();

            nearVers = new ArrayList<>();
        }

        nearKeysBytes.add(keyBytes);

        nearVers.add(ver);
    }

    /**
     * @return Keys.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @return Versions.
     */
    public List<GridCacheVersion > versions() {
        return vers;
    }

    /**
     * @param idx Entry index.
     * @return Version.
     */
    public GridCacheVersion version(int idx) {
        assert idx >= 0 && idx < vers.size() : idx;

        return vers.get(idx);
    }

    /**
     * @return Keys for near cache.
     */
    public List<K> nearKeys() {
        return nearKeys;
    }

    /**
     * @return Versions for near cache entries.
     */
    public List<GridCacheVersion > nearVersions() {
        return nearVers;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null && keysBytes != null)
            keys = unmarshalCollection(keysBytes, ctx, ldr);

        if (nearKeys == null && nearKeysBytes != null)
            nearKeys = unmarshalCollection(nearKeysBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 82;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheTtlUpdateRequest _clone = new GridCacheTtlUpdateRequest();

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
            case 3:
                if (keysBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(keysBytes.size()))
                            return false;

                        commState.it = keysBytes.iterator();
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
                if (nearKeysBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(nearKeysBytes.size()))
                            return false;

                        commState.it = nearKeysBytes.iterator();
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

            case 5:
                if (nearVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(nearVers.size()))
                            return false;

                        commState.it = nearVers.iterator();
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

            case 6:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putLong(ttl))
                    return false;

                commState.idx++;

            case 8:
                if (vers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(vers.size()))
                            return false;

                        commState.it = vers.iterator();
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

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 3:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (keysBytes == null)
                        keysBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        keysBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (nearKeysBytes == null)
                        nearKeysBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        nearKeysBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (nearVers == null)
                        nearVers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        nearVers.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 7:
                if (buf.remaining() < 8)
                    return false;

                ttl = commState.getLong();

                commState.idx++;

            case 8:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (vers == null)
                        vers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        vers.add((GridCacheVersion)_val);

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
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheTtlUpdateRequest _clone = (GridCacheTtlUpdateRequest)_msg;

        _clone.keys = keys;
        _clone.keysBytes = keysBytes;
        _clone.vers = vers;
        _clone.nearKeys = nearKeys;
        _clone.nearKeysBytes = nearKeysBytes;
        _clone.nearVers = nearVers;
        _clone.ttl = ttl;
        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTtlUpdateRequest.class, this, "super", super.toString());
    }
}
