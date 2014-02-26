// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.query.*;

import java.util.*;

/**
 * TODO
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueriesEx<K, V> extends GridCacheQueries<K, V> {
    public Collection<GridCacheQueryMetrics> metrics();

    public void resetMetrics();
}
