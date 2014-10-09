/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Entry for batch swap operations.
 */
public class GridCacheBatchSwapEntry<K, V> extends GridCacheSwapEntryImpl<V> {
    /** Key. */
    private K key;

    /** Key bytes. */
    private byte[] keyBytes;

    /** Partition. */
    private int part;

    /**
     * Creates batch swap entry.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param part Partition id.
     * @param valBytes Value bytes.
     * @param valIsByteArr Whether value is byte array.
     * @param ver Version.
     * @param ttl Time to live.
     * @param expireTime Expire time.
     * @param keyClsLdrId Key class loader ID.
     * @param valClsLdrId Optional value class loader ID.
     */
    public GridCacheBatchSwapEntry(K key,
        byte[] keyBytes,
        int part,
        byte[] valBytes,
        boolean valIsByteArr,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        GridUuid keyClsLdrId,
        @Nullable GridUuid valClsLdrId) {
        super(valBytes, valIsByteArr, ver, ttl, expireTime, keyClsLdrId, valClsLdrId);

        this.key = key;
        this.keyBytes = keyBytes;
        this.part = part;
    }

    /**
     * @return Key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /**
     * @return Partition id.
     */
    public int partition() {
        return part;
    }
}
