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
     * @return Entry bytes.
     */
    public byte[] marshal() {
        int size = 16 + 1 + 24; // Ttl + expire time + Ex Version flag + Version.

        if (ver instanceof GridCacheVersionEx)
            size += 24;

        size += 1; // Plain byte array flag.

        size += valBytes.length + 4; // Value bytes.

        size += (valClsLdrId == null ? 1 : 24);

        size += (keyClsLdrId == null ? 1 : 24);

        byte[] arr = new byte[size];

        long off = BYTE_ARR_OFF;

        UNSAFE.putLong(arr, off, ttl);

        off += 8;

        UNSAFE.putLong(arr, off, expireTime);

        off += 8;

        off = U.writeVersion(arr, off, ver);

        UNSAFE.putBoolean(arr, off++, valIsByteArr);

        UNSAFE.putInt(arr, off, valBytes.length);

        off += 4;

        UNSAFE.copyMemory(valBytes, BYTE_ARR_OFF, arr, off, valBytes.length);

        off += valBytes.length;

        off = U.writeGridUuid(arr, off, valClsLdrId);

        U.writeGridUuid(arr, off, keyClsLdrId);

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

        GridCacheVersion ver = U.readVersion(arr, off, verEx);

        off += verEx ? 48 : 24;

        boolean valIsByteArr = UNSAFE.getBoolean(arr, off++);

        int arrLen = UNSAFE.getInt(arr, off);

        off += 4;

        byte[] valBytes = new byte[arrLen];

        UNSAFE.copyMemory(arr, off, valBytes, BYTE_ARR_OFF, arrLen);

        off += arrLen;

        GridUuid valClsLdrId = U.readGridUuid(arr, off);

        off += valClsLdrId == null ? 1 : 25;

        GridUuid keyClsLdrId = U.readGridUuid(arr, off);

        return new GridCacheSwapEntryImpl<T>(valBytes,
            valIsByteArr,
            ver,
            ttl,
            expireTime,
            keyClsLdrId,
            valClsLdrId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSwapEntryImpl.class, this);
    }
}
