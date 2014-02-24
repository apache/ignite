// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Fields query base adapter to overcome generic multiple inheritance issue.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridCacheFieldsQueryBase<K, V, T extends GridCacheQueryBase>
    extends GridCacheQueryBaseAdapter<K, V, T> {
    /** Include meta data or not. */
    private boolean incMeta;

    /**
     * @param cctx Cache context.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Queried data type.
     * @param clsName Queried data type class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheFieldsQueryBase(GridCacheContext<K, V> cctx, @Nullable GridCacheQueryType type,
        @Nullable String clause, @Nullable Class<?> cls, @Nullable String clsName,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        super(cctx, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param cctx Cache context.
     * @param qryId Query ID.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Queried data type.
     * @param clsName Queried data type class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheFieldsQueryBase(GridCacheContext<K, V> cctx, int qryId, @Nullable GridCacheQueryType type,
        @Nullable String clause, @Nullable Class<?> cls, @Nullable String clsName,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        super(cctx, qryId, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param qry Query to copy.
     */
    protected GridCacheFieldsQueryBase(GridCacheFieldsQueryBase<K, V, T> qry) {
        super(qry);

        incMeta = qry.incMeta;
    }

    /**
     * @return Flag indicating whether query metadata should be included or not.
     */
    public boolean includeMetadata() {
        return incMeta;
    }

    /**
     * @param incMeta Flag indicating whether query metadata should be included or not.
     */
    public void includeMetadata(boolean incMeta) {
        this.incMeta = incMeta;
    }
}
