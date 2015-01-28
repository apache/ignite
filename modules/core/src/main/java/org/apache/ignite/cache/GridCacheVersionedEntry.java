package org.apache.ignite.cache;

import org.jetbrains.annotations.*;

/**
 * Cache entry along with version information.
 */
public interface GridCacheVersionedEntry<K, V> {
    /**
     * Gets entry's key.
     *
     * @return Entry's key.
     */
    public K key();

    /**
     * Gets entry's value.
     *
     * @return Entry's value.
     */
    @Nullable
    public V value();

    /**
     * Gets entry's TTL.
     *
     * @return Entry's TTL.
     */
    public long ttl();

    /**
     * Gets entry's expire time.
     *
     * @return Entry's expire time.
     */
    public long expireTime();

    /**
     * Gets ID of initiator data center.
     *
     * @return ID of initiator data center.
     */
    public byte dataCenterId();

    /**
     * Gets entry's topology version in initiator data center.
     *
     * @return Entry's topology version in initiator data center.
     */
    public int topologyVersion();

    /**
     * Gets entry's order in initiator data center.
     *
     * @return Entry's order in initiator data center
     */
    public long order();

    /**
     * Gets entry's global time in initiator data center.
     *
     * @return Entry's global time in initiator data center
     */
    public long globalTime();
}
