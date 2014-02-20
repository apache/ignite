// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dataload;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Entry for data loader.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDataLoadEntry<K, V> {
    /**
     * Gets the key of entry.
     *
     * @return Entry key.
     * @throws GridException If failed.
     */
    public K key() throws GridException;

    /**
     * Gets the new value for the key, probably {@code null}.
     *
     * Bundled implementations of cache updater in ({@link GridDataLoadCacheUpdaters}
     * interpret {@code null} value as a command to remove respective entry from cache.
     *
     * @return New value.
     * @throws GridException If failed.
     */
    @Nullable public V value() throws GridException;
}
