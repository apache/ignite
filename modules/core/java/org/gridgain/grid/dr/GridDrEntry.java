// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr;

import org.jetbrains.annotations.*;

/**
 * Data center replication entry.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDrEntry<K, V> {
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
    @Nullable public V value();

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
