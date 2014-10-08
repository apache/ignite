// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.queries;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import javax.cache.event.*;
import java.util.*;

import static javax.cache.Cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Queries<K, V> {
    public QueryConfiguration configuration();

    public Queries<K, V> withConfiguration(QueryConfiguration cfg);

    public Iterator<Entry<K, V>> scan(GridPredicate<Entry<K, V>> filter) throws GridException;

    public <R> Iterator<R> scanReduce(GridReducer<Entry<K, V>, R> rmtRdc,
        GridPredicate<Entry<K, V>> filter) throws GridException;

    public <R> Iterator<Entry<K, R>> scanTransform(GridClosure<Entry<K, V>, Entry<K, R>> rmtTransform,
        GridPredicate<Entry<K, V>> filter) throws GridException;

    public Iterator<Entry<K, V>> sql(String sql, Object... args) throws GridException;

    public <R> Iterator<R> sqlReduce(GridReducer<Entry<K, V>, R> rmtRdc, String sql, Object... args);

    public <R> Iterator<R> sqlTransform(GridClosure<Entry<K, V>, Entry<K, R>> rmtTransform, String sql, Object... args);

    public Iterator<List<?>> sqlFields(String sql, Object... args) throws GridException;

    public <R> Iterator<R> sqlFieldsReduce(GridReducer<List<?>, R> rmtRdc, String sql, Object... args) throws GridException;

    public Iterator<Entry<K, V>> text(String txt) throws GridException;

    public <R> Iterator<R> textReduce(GridReducer<Entry<K, V>, R> rmtRdc, String txt) throws GridException;

    public <R> Iterator<Entry<K, R>> textTransform(GridClosure<Entry<K, V>, Entry<K, R>> rmtTransform, String txt)
        throws GridException;

    public void continuous(CacheEntryUpdatedListener<K, V> locCb, CacheEntryEventFilter<K, V> rmtFilter)
        throws GridException;

    public QueryMetrics metrics();

    public void resetMetrics();

    /**
     * Forces this cache to rebuild all search indexes of given value type. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @param typeName Value type name to rebuild indexes for.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    // TODO: do we need this?
    public GridFuture<?> rebuildIndexes(String typeName);

    /**
     * Forces this cache to rebuild search indexes of all types. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    // TODO: do we need this?
    public GridFuture<?> rebuildAllIndexes();
}
