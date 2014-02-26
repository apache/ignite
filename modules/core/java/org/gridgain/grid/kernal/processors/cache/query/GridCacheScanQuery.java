// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.query.GridCacheQueryType.*;

/**
 * TODO
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheScanQuery<K, V> extends GridCacheQueryAdapter<Map.Entry<K, V>> {
    /**
     * @param ctx Context.
     * @param prjPred Cache projection predicate.
     */
    public GridCacheScanQuery(GridCacheContext<?, ?> ctx, @Nullable GridPredicate<GridCacheEntry<?, ?>> prjPred) {
        super(ctx, SCAN, prjPred);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheQueryAdapter<Map.Entry<K, V>> copy0() {
        return new GridCacheScanQuery<>(ctx, prjPred);
    }
}
