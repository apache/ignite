// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query with possible remote transformer. The execution sequence is
 * essentially identical to the one described in {@link GridCacheQuery} javadoc,
 * except that queried values are given to an optional transformer
 * directly on the queried node which should usually transform cached values into
 * smaller and more light-weight objects to return to caller. This technique allows
 * to save on network trips whenever only a subset of data from cached values needs
 * to be returned.
 * <p>
 * Transform queries are created from {@link GridCacheProjection} API via any
 * of the available {@code createTransformQuery(...)} methods.
 * <h1 class="header">Transform Query Usage</h1>
 * Here is a query example which returns only employee names to the caller instead
 * of returning full {@code 'Person'} objects with lots of extra data.
 * <pre name="code" class="java">
 * GridCache&lt;Long, Person&gt; cache = G.grid().cache();
 * ...
 * // Create query to get names of all employees working for some company.
 * GridCacheTransformQuery&lt;UUID, Person, String&gt; qry =
 *   cache.createTransformQuery(SQL, Person.class,
 *     "from Person, Organization where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");
 *
 * // Transformer to convert Person objects to String.
 * // Since caller only needs employee names, we only
 * // send names back.
 * qry.remoteTransformer(new CO&lt;GridClosure&lt;Person, String&gt;&gt;() {
 *     &#64;Override public GridClosure&lt;Person, String&gt; apply() {
 *         return new C1&lt;Person, String&gt;() {
 *             &#64;Override public String apply(Person person) {
 *                 return person.getName();
 *             }
 *         };
 *     }
 * });
 *
 * // Query all nodes for names of all GridGain employees.
 * Collection&lt;String&gt; gridgainEmployeeNames = qry.with("GridGain").execute(G.grid()).get());
 * </pre>
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @param <T> Transformed value type.
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheTransformQuery<K, V, T> extends GridCacheQueryBase<K, V> {
    /**
     * Optional arguments that get passed into query SQL.
     *
     * @param args Optional query arguments.
     * @return This query with the passed in arguments preset.
     */
    public GridCacheTransformQuery<K, V, T> queryArguments(@Nullable Object... args);

    /**
     * Optional arguments for closures to be used by {@link #remoteKeyFilter(GridClosure)},
     * {@link #remoteValueFilter(GridClosure)}, and {@link #remoteTransformer(GridClosure)}.
     *
     * @param args Optional query arguments.
     * @return This query with the passed in arguments preset.
     */
    public GridCacheTransformQuery<K, V, T> closureArguments(@Nullable Object... args);

    /**
     * Sets optional transformer factory to transform values returned from queried nodes.
     * Transformer factory is a closure which accepts array of objects provided
     * by {@link #closureArguments(Object...)} method as a parameter and returns a closure
     * that transforms one value into another. Transformers are especially useful whenever
     * only a subset of queried values needs to be returned to caller and can help save on
     * network overhead. Transformer will usually take the queried values and return
     * smaller, more light weight values to the caller.
     * <p>
     * If factory is set, then it will be invoked for every query execution. If state of
     * the transformer closure changes every time a query is executed, then factory should
     * return a new transformer closure for every execution.
     *
     * @param factory Optional transformer factory to transform values on queried nodes.
     */
    public void remoteTransformer(@Nullable GridClosure<Object[], GridClosure<V, T>> factory);

    /**
     * Executes the query and returns the first result in the result set. If more
     * than one key-value pair are returned, then will be ignored.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Optional subgrid projection to execute this query on (if not provided, then the whole grid is used).
     * @return Future for the single query result.
     */
    public GridFuture<Map.Entry<K, T>> executeSingle(GridProjection... grid);

    /**
     * Synchronously executes the query and returns the first result in the result set. If more
     * than one key-value pair are returned, then will be ignored.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Optional subgrid projection to execute this query on (if not provided, then the whole grid is used).
     * @return Single query result.
     * @throws GridException In case of error.
     */
    public Map.Entry<K, T> executeSingleSync(GridProjection... grid) throws GridException;

    /**
     * Executes the query and returns the query future. Caller may decide to iterate
     * over the returned future directly in which case the iterator may block until
     * the next value will become available, or wait for the whole query to finish
     * by calling any of the {@code 'get(..)'} methods on the returned future. If
     * {@link #keepAll(boolean)} flag is set to {@code false}, then {@code 'get(..)'}
     * methods will only return the last page received, otherwise all pages will be
     * accumulated and returned to user as a collection.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Optional subgrid projection to execute this query on (if not provided, then the whole grid is used).
     * @return Future for the query result.
     */
    public GridCacheQueryFuture<Map.Entry<K, T>> execute(GridProjection... grid);

    /**
     * Synchronously executes the query and returns query result. If
     * {@link #keepAll(boolean)} flag is set to {@code false}, then the
     * method will only return the last page received, otherwise all pages will be
     * accumulated and returned to user as a collection.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Optional subgrid projection to execute this query on (if not provided, then the whole grid is used).
     * @return Query result.
     * @throws GridException In case of error.
     */
    public Collection<Map.Entry<K, T>> executeSync(GridProjection... grid) throws GridException;
}
