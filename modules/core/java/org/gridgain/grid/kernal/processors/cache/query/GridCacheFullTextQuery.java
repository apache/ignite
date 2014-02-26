// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
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
public class GridCacheFullTextQuery<K, V> extends GridCacheQueryAdapter<Map.Entry<K, V>> {
    /** */
    private final Class<? extends V> cls;

    /** */
    private final String search;

    /**
     * @param ctx Context.
     * @param prjPred Cache projection predicate.
     * @param cls Class.
     * @param search Search string.
     */
    public GridCacheFullTextQuery(GridCacheContext<?, ?> ctx, @Nullable GridPredicate<GridCacheEntry<?, ?>> prjPred,
        Class<? extends V> cls, String search) {
        super(ctx, TEXT, prjPred);

        assert cls != null;
        assert search != null;

        this.cls = cls;
        this.search = search;
    }

    /**
     * @return Query class.
     */
    public Class<? extends V> queryClass() {
        return cls;
    }

    /**
     * @return Search string.
     */
    public String search() {
        return search;
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        ctx.deploy().registerClass(cls);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheQueryAdapter<Map.Entry<K, V>> copy0() {
        return new GridCacheFullTextQuery<>(ctx, prjPred, cls, search);
    }
}
