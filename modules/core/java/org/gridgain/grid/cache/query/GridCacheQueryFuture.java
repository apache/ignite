// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query future returned by query execution.
 * Refer to corresponding query documentation for more information.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueryFuture<X> extends GridFuture<Collection<X>> {
    /**
     * @return Number of fetched elements which are available immediately.
     */
    public int available() throws GridException;

    /**
     * Blocking call
     *
     * @return Next fetched element or {@code null} if all the elements.
     * @throws GridException If failed.
     */
    @Nullable public X next() throws GridException;

    /**
     * Checks if all data is fetched by the query.
     *
     * @return {@code True} if all data is fetched, {@code false} otherwise.
     */
    @Override public boolean isDone();

    /**
     * Cancels this query future and stop receiving any further results for the query
     * associated with this future.
     *
     * @return {@inheritDoc}
     * @throws GridException {@inheritDoc}
     */
    @Override public boolean cancel() throws GridException;

    /**
     * Gets future for received result metadata. Metadata describes fields included
     * in result set, describing their names, types, etc.
     * <p>
     * If {@link GridCacheFieldsQuery#includeMetadata(boolean)} flag is {@code true}, future
     * is blocked until first result page is received. Otherwise, future is
     * initially in {@code done} state and returns {@code null}.
     * <p>
     * Note that even if {@link GridCacheFieldsQuery#includeMetadata(boolean)} flag is
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
