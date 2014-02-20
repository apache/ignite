// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.io.*;
import java.util.*;

/**
 * Updates cache with batch of {@link GridDataLoadEntry}.
 * Data loader can be configured to use custom implementation of updater instead of default one using
 * {@link GridDataLoader#updater(GridDataLoadCacheUpdater)} method. Bundled implementations can be found in
 * {@link GridDataLoadCacheUpdaters} factory class.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDataLoadCacheUpdater<K, V> extends Serializable {
    /**
     * Updates cache with batch of entries.
     *
     * @param cache Cache.
     * @param entries Collection of entries.
     * @throws GridException If failed.
     */
    public void update(GridCache<K, V> cache, Collection<GridDataLoadEntry<K, V>> entries) throws GridException;
}
