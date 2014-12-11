/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.dataload;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;

import java.io.*;
import java.util.*;

/**
 * Updates cache with batch of entries. Usually it is enough to configure {@link org.apache.ignite.IgniteDataLoader#isolated(boolean)}
 * property and appropriate internal cache updater will be chosen automatically. But in some cases to achieve best
 * performance custom user-defined implementation may help.
 * <p>
 * Data loader can be configured to use custom implementation of updater instead of default one using
 * {@link org.apache.ignite.IgniteDataLoader#updater(IgniteDataLoadCacheUpdater)} method.
 */
public interface IgniteDataLoadCacheUpdater<K, V> extends Serializable {
    /**
     * Updates cache with batch of entries.
     *
     * @param cache Cache.
     * @param entries Collection of entries.
     * @throws IgniteCheckedException If failed.
     */
    public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteCheckedException;
}
