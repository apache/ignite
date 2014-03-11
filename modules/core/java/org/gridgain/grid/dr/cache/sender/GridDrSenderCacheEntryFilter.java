/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.sender;

import org.gridgain.grid.dr.*;

/**
 * Data center replication sender cache filter. Prevents data center replication of cache entries which do not pass it.
 */
public interface GridDrSenderCacheEntryFilter<K, V> {
    /**
     * Whether to perform data center replication for particular cache entry or not.
     *
     * @param entry DR entry.
     * @return {@code True} in case data center replication should be performed for the entry..
     */
    public boolean accept(GridDrEntry<K, V> entry);
}
