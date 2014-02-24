// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Cache query future returned by {@link GridCacheFieldsQuery#execute()} method.
 * Note that this future is different from {@link GridCacheQueryFuture} only with
 * addition of {@link #metadata()} method which provides field descriptors for
 * all returned fields.
 * <p>
 * Please refer to {@link GridCacheFieldsQuery} javadoc documentation for more information.
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheFieldsQuery
 */
public interface GridCacheFieldsQueryFuture extends GridCacheQueryFuture<List<Object>> {
    /**
     * Gets future for received result metadata. Metadata describes fields included
     * in result set, describing their names, types, etc.
     * <p>
     * If {@link GridCacheFieldsQuery#includeMetadata()} flag is {@code true}, future
     * is blocked until first result page is received. Otherwise, future is
     * initially in {@code done} state and returns {@code null}.
     * <p>
     * Note that even if {@link GridCacheFieldsQuery#includeMetadata()} flag is
     * {@code true}, you can also get {@code null} metadata. This happens when your query
     * failed locally on each node with {@code table not found} error. If you request only
     * one data type, check it's name. Otherwise, this can mean that your query is invalid
     * or that there is no data you requested, depending on how is data is collocated in
     * cache.
     *
     * @return Meta data.
     */
    public GridFuture<List<GridCacheSqlFieldMetadata>> metadata();
}
