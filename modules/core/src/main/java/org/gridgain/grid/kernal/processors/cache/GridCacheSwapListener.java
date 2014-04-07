/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

/**
 * Provides ability to listen to swap events in cache which is necessary for preloading.
 */
public interface GridCacheSwapListener<K, V> {
    /**
     * @param part Partition.
     * @param key Cache key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public void onEntryUnswapped(int part, K key, byte[] keyBytes, V val, byte[] valBytes,
        GridCacheVersion ver, long ttl, long expireTime);
}
