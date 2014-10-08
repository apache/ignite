/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.util.*;

/**
 * Swap entry.
 */
public class GridCacheSwapEntryImpl<V> implements GridCacheSwapEntry<V> {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** Value bytes. */
    private byte[] valBytes;

    /** Value. */
    private V val;

    /** Falg indicating that value is byte array, so valBytes should not be unmarshalled. */
    private boolean valIsByteArr;

    /** Class loader ID. */
    private GridUuid keyClsLdrId;

    /** Class loader ID. */
    private GridUuid valClsLdrId;

    /** Version. */
    private GridCacheVersion ver;

    /** Time to live. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * @param valBytes Value.
     * @param valIsByteArr Whether value of this entry is byte array.
     * @param ver Version.
     * @param ttl Entry time to live.
     * @param expireTime Expire time.
     * @param keyClsLdrId Class loader ID for entry key (can be {@code null} for local class loader).
     * @param valClsLdrId Class loader ID for entry value (can be {@code null} for local class loader).
     */
    public GridCacheSwapEntryImpl(
        byte[] valBytes,
        boolean valIsByteArr,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        @Nullable GridUuid keyClsLdrId,
        @Nullable GridUuid valClsLdrId) {
        assert ver != null;

        this.valBytes = valBytes;
        this.valIsByteArr = valIsByteArr;
        this.ver = ver;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.valClsLdrId = valClsLdrId;
        this.keyClsLdrId = keyClsLdrId;
    }


    /** {@inheritDoc} */
    @Override public byte[] valueBytes() {
        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(V val) {
        this.val = val;

        if (val instanceof byte[])
            valBytes = null;
    }

    /** {@inheritDoc} */
    @Override public boolean valueIsByteArray() {
        return valIsByteArr;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid keyClassLoaderId() {
        return keyClsLdrId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid valueClassLoaderId() {
        return valClsLdrId;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /**
     * @param portable Portable enabled flag.
     * @return Entry bytes.
     */
    public byte[] marshal(boolean portable) {
        int size = 16 + 1 + 24; // Ttl + expire time + Ex Version flag + Version.

        if (ver instanceof GridCacheVersionEx)
            size += 24;

        size += 1; // Plain byte array flag.

        size += valBytes.length + 4; // Value bytes.

        byte[] arr = new byte[size];

        long off = BYTE_ARR_OFF;

        UNSAFE.putLong(arr, off, ttl);

        off += 8;

        UNSAFE.putLong(arr, off, expireTime);

        off += 8;

        off = writeVersion(arr, off, ver, true);

        UNSAFE.putBoolean(arr, off++, valIsByteArr);

        UNSAFE.putInt(arr, off, valBytes.length);

        off += 4;

        UNSAFE.copyMemory(valBytes, BYTE_ARR_OFF, arr, off, valBytes.length);

        return arr;
    }

    /**
     * @param arr Entry bytes.
     * @return Entry.
     */
    public static <T> GridCacheSwapEntryImpl<T> unmarshal(byte[] arr) {
        long off = BYTE_ARR_OFF;

        long ttl = UNSAFE.getLong(arr, off);

        off += 8;

        long expireTime = UNSAFE.getLong(arr, off);

        off += 8;

        boolean verEx = UNSAFE.getBoolean(arr, off++);

        GridCacheVersion ver = readVersion(arr, off, verEx);

        off += verEx ? 48 : 24;

        boolean valIsByteArr = UNSAFE.getBoolean(arr, off++);

        int arrLen = UNSAFE.getInt(arr, off);

        off += 4;

        byte[] valBytes = new byte[arrLen];

        UNSAFE.copyMemory(arr, off, valBytes, BYTE_ARR_OFF, arrLen);

        return new GridCacheSwapEntryImpl<T>(valBytes, valIsByteArr, ver, ttl, expireTime, null, null);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param uid UUID.
     * @return Offset.
     */
    private long writeGridUuid(byte[] arr, long off, @Nullable GridUuid uid) {
        UNSAFE.putBoolean(arr, off++, uid != null);

        if (uid != null) {
            UNSAFE.putLong(arr, off, uid.globalId().getMostSignificantBits());

            off += 8;

            UNSAFE.putLong(arr, off, uid.globalId().getLeastSignificantBits());

            off += 8;

            UNSAFE.putLong(arr, off, uid.localId());

            off += 8;
        }

        return off;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param ver Version.
     * @param checkEx If {@code true} checks if version is {@link GridCacheVersionEx}.
     * @return Offset.
     */
    private long writeVersion(byte[] arr, long off, GridCacheVersion ver, boolean checkEx) {
        boolean verEx = false;

        if (checkEx) {
            verEx = ver instanceof GridCacheVersionEx;

            UNSAFE.putBoolean(arr, off++, verEx);
        }

        if (verEx) {
            writeVersion(arr, off, ver.drVersion(), false);

            off += 24;
        }

        UNSAFE.putInt(arr, off, ver.topologyVersion());

        off += 4;

        UNSAFE.putInt(arr, off, ver.nodeOrderAndDrIdRaw());

        off += 4;

        UNSAFE.putLong(arr, off, ver.globalTime());

        off += 8;

        UNSAFE.putLong(arr, off, ver.order());

        off += 8;

        return off;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param verEx If {@code true} reads {@link GridCacheVersionEx} instance.
     * @return Version.
     */
    private static GridCacheVersion readVersion(byte[] arr, long off, boolean verEx) {
        int topVer = UNSAFE.getInt(arr, off);

        off += 4;

        int nodeOrderDrId = UNSAFE.getInt(arr, off);

        off += 4;

        long globalTime = UNSAFE.getLong(arr, off);

        off += 8;

        long order = UNSAFE.getLong(arr, off);

        off += 8;

        GridCacheVersion ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);

        if (verEx) {
            topVer = UNSAFE.getInt(arr, off);

            off += 4;

            nodeOrderDrId = UNSAFE.getInt(arr, off);

            off += 4;

            globalTime = UNSAFE.getLong(arr, off);

            off += 8;

            order = UNSAFE.getLong(arr, off);

            ver = new GridCacheVersionEx(topVer, nodeOrderDrId, globalTime, order, ver);
        }

        return ver;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSwapEntryImpl.class, this);
    }
}
