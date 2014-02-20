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
 * Cache query with possible remote and local reducers.
 * <p>
 * This query type supports most of concepts and properties supported by {@link GridCacheQuery}.
 * Refer to {@link GridCacheQuery} documentation for more information.
 * <p>
 * This query combines abilities of {@link GridCacheFieldsQuery} and {@link GridCacheReduceQuery}:
 * individual fields in query {@code 'select ...'} clause are given to an optional reducer
 * directly on the queried node and a single reduced value is returned back
 * to caller node. Then on the caller node, the collection of reduced values
 * is given to optionally provided local reducer. Based on whether local
 * reducer is provided or not, either a single value or a collection of
 * reduced values is returned to user.
 * <p>
 * Reduce queries are created from {@link GridCacheProjection} API via {@code createReduceFieldsQuery(...)} method.
 * <h1 class="header">Reduce Fields Query Usage</h1>
 * Here is a query example which calculates average salary for all cached employees of
 * any given company based on the same example data mode described in {@link GridCacheQuery}
 * documentation.
 * <pre name="code" class="java">
 *    GridCache&lt;String, Organization> cache = G.grid().cache();
 *
 *    // Calculate average of salary of all employees of all companies.
 *    GridCacheReduceFieldsQuery&lt;GridTuple2&lt;Integer, Integer&gt;, Integer&gt; qry = cache.createReduceFieldsQuery(
 *        "select age from Person");
 *
 *    // Set remote reducer to calculate sum of salaries and employee count on remote nodes.
 *    qry.remoteReducer(new C1&lt;Object[], GridReducer&lt;List&lt;Object&gt;, GridTuple2&lt;Integer, Integer&gt;&gt;&gt;() {
 *        private final GridReducer&lt;List&lt;Object&gt;, GridTuple2&lt;Integer, Integer&gt;&gt; rdc =
 *            new GridReducer&lt;List&lt;Object&gt;, GridTuple2&lt;Integer, Integer&gt;&gt;() {
 *                private int sum;
 *
 *                private int cnt;
 *
 *                &#64;Override public boolean collect(List&lt;Object&gt; e) {
 *                    sum += (Integer)e.get(0);
 *
 *                    cnt++;
 *
 *                    return true;
 *                }
 *
 *                &#64;Override public GridTuple2&lt;Integer, Integer&gt; apply() {
 *                    return F.t(sum, cnt);
 *                }
 *            };
 *
 *        &#64;Override public GridReducer&lt;List&lt;Object&gt;, GridTuple2&lt;Integer, Integer&gt;&gt; apply(Object[] args) {
 *            return rdc;
 *        }
 *    });
 *
 *    // Set local reducer to reduce totals from queried nodes into overall average.
 *    qry.localReducer(new C1&lt;Object[], GridReducer&lt;GridTuple2&lt;Integer, Integer&gt;, Integer&gt;&gt;() {
 *        private final GridReducer&lt;GridTuple2&lt;Integer, Integer&gt;, Integer&gt; rdc =
 *            new GridReducer&lt;GridTuple2&lt;Integer, Integer&gt;, Integer&gt;() {
 *                private int sum;
 *
 *                private int cnt;
 *
 *                &#64;Override public boolean collect(GridTuple2&lt;Integer, Integer&gt; t) {
 *                    sum += t.get1();
 *                    cnt += t.get2();
 *
 *                    return true;
 *                }
 *
 *                &#64;Override public Integer apply() {
 *                    return cnt == 0 ? 0 : sum / cnt;
 *                }
 *            };
 *
 *        &#64;Override public GridReducer&lt;GridTuple2&lt;Integer, Integer&gt;, Integer&gt; apply(Object[] args) {
 *            return rdc;
 *        }
 *    });
 *
 *    // Query all nodes to find average salary of all employees of all companies.
 *    int avg = qry.reduce(G.grid()).get();
 * </pre>
 *
 * @param <R1> Remotely reduced type.
 * @param <R2> Locally reduced type.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheReduceFieldsQuery<R1, R2, K, V> extends GridCacheQueryBase<K, V> {
    /**
     * Optional remote reducer factory to provide reducers for reduction of multiple
     * queried values on queried nodes into one. The factory is a closure which
     * accepts array of objects provided by {@link #closureArguments(Object...)}
     * method and returns reducer to reduce queried values.
     * <p>
     * If factory is set, then it should provide a new instance of reducer for every
     * query execution.
     *
     * @param factory Optional remote reducer factory to create reducers for use on queried nodes.
     */
    public void remoteReducer(@Nullable GridClosure<Object[], GridReducer<List<Object>, R1>> factory);

    /**
     * Optional local reducer factory to provide reducers for reduction of multiple queried values
     * returned from remote nodes into one. The factory is a closure which accepts array of objects provided
     * by {@link #closureArguments(Object...)} method ar parameter and returns reducer to locally reduce
     * multiple query results returned from remote nodes into one.
     * <p>
     * If factory is set, then it should provide a new instance of reducer for every query execution.
     *
     * @param factory Optional reducer factory to create local reducers to reduce query results returned
     *      from queried nodes.
     */
    public void localReducer(@Nullable GridClosure<Object[], GridReducer<R1, R2>> factory);

    /**
     * Optional query arguments that get passed to query SQL.
     *
     * @param args Optional query arguments.
     * @return This query with the passed in arguments preset.
     */
    public GridCacheReduceFieldsQuery<R1, R2, K, V> queryArguments(@Nullable Object... args);

    /**
     * Optional arguments for closures to be used by {@link #remoteKeyFilter(GridClosure)},
     * {@link #remoteValueFilter(GridClosure)}, {@link #remoteReducer(GridClosure)}, and
     * {@link #localReducer(GridClosure)}.
     *
     * @param args Optional query arguments.
     * @return This query with the passed in arguments preset.
     */
    public GridCacheReduceFieldsQuery<R1, R2, K, V> closureArguments(@Nullable Object... args);

    /**
     * Executes query on the given grid projection using remote and local reducers and
     * returns a future for the queried result.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Grid projection to execute query on, if not provided, all grid nodes will be used.
     * @return Future for the reduced query result.
     */
    public GridFuture<R2> reduce(@Nullable GridProjection... grid);

    /**
     * Synchronously executes query on the given grid projection using remote and local reducers and
     * returns queried result.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Grid projection to execute query on, if not provided, all grid nodes will be used.
     * @return Reduced query result.
     * @throws GridException In case of error.
     */
    public R2 reduceSync(@Nullable GridProjection... grid) throws GridException;

    /**
     * Executes query on the given grid projection using remote reducer and
     * returns a future for the queried result. The result is a collection of
     * reduced values returned from queried nodes.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Grid projection to execute query on, if not provided, all grid nodes will be used.
     * @return Future for the reduced query result.
     */
    public GridFuture<Collection<R1>> reduceRemote(@Nullable GridProjection... grid);

    /**
     * Synchronously executes query on the given grid projection using remote reducer and
     * returns queried result. The result is a collection of
     * reduced values returned from queried nodes.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param grid Grid projection to execute query on, if not provided, all grid nodes will be used.
     * @return Reduced query result.
     * @throws GridException In case of error.
     */
    public Collection<R1> reduceRemoteSync(@Nullable GridProjection... grid) throws GridException;
}
