// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query that returns collection of individually selected fields instead of key-value pairs.
 * <p>
 * This query type supports most of concepts and properties supported by {@link GridCacheQuery}.
 * Refer to {@link GridCacheQuery} documentation for more information.
 * <p>
 * The main difference of {@code GridCacheFieldsQuery} vs {@link GridCacheQuery} is
 * ability to specify individual fields in query {@code 'select ...'} clause, while
 * this clause would be ignored by {@link GridCacheQuery} since it would always return
 * full objects, not their fields.
 * <p>
 * <h2 class="header">Usage example</h2>
 * As an example, suppose we have a cache that stores instances of
 * {@code 'Person'} class defined as follows:
 * <pre name="code" class="java">
 * public class Person {
 *     // Name.
 *     &#64;GridCacheQuerySqlField(index = false)
 *     private final String name;
 *
 *     // Age.
 *     &#64;GridCacheQuerySqlField
 *     private final int age;
 *
 *     ...
 * }
 * </pre>
 * Then you can query all persons older than 30 years and get only their names:
 * <pre name="code" class="java">
 * GridCache&lt;Long, Person&gt; cache = G.grid().cache();
 *
 * // Create query.
 * GridCacheFieldsQuery qry = cache.createFieldsQuery("select name from Person where age &gt; ?");
 *
 * // Set argument and execute on all nodes in grid.
 * Collection&lt;List&lt;Object&gt;&gt; res = qry.queryArguments(30).execute(G.grid()).get();
 *
 * // Get value from row.
 * for (List&ltObject&gt; row : res)
 *     System.out.println(row.get(0));
 * </pre>
 * Or get person's age by his name:
 * <pre name="code" class="java">
 * GridCache&lt;Long, Person&gt; cache = G.grid().cache();
 *
 * // Create query.
 * GridCacheFieldsQuery qry = cache.createFieldsQuery("select age from Person where name = ?");
 *
 * // You can use convenient 'executeSingleField' method here.
 * Integer age = qry.queryArguments("John Doe").&lt;Integer&gt;executeSingleField(G.grid()).get();
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheFieldsQuery<K, V> extends GridCacheQueryBase<K, V, GridCacheFieldsQuery<K, V>> {
    /**
     * Gets {@code includeMetadata} flag.
     *
     * @return {@code includeMetadata} flag.
     */
    public boolean includeMetadata();

    /**
     * Sets whether or not include metadata for received result.
     * If {@code true}, metadata is available after first page
     * is received and can be obtained from {@link GridCacheFieldsQueryFuture#metadata()} method.
     * <p>
     * Default value is {@code false}.
     *
     * @param incMeta Include meta data or not.
     */
    public void includeMetadata(boolean incMeta);

    /**
     * Sets optional query arguments.
     *
     * @param args Optional query arguments.
     * @return This query.
     */
    public GridCacheFieldsQuery queryArguments(@Nullable Object... args);

    /**
     * Executes the query and returns the query future. Caller may decide to iterate
     * over the returned future directly in which case the iterator may block until
     * the next value will become available, or wait for the whole query to finish
     * by calling any of the {@code 'get(..)'} methods on the returned future. If
     * {@link #keepAll(boolean)} flag is set to {@code false}, then {@code 'get(..)'}
     * methods will only return the last page received, otherwise all pages will be
     * accumulated and returned to user.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @return Future for the query result.
     * @see GridCacheFieldsQueryFuture
     */
    public GridCacheFieldsQueryFuture execute();

    /**
     * Executes the query and returns the first result in the result set.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.) if this
     * method was called at least once.
     *
     * @return Future for the single query result.
     */
    public GridFuture<List<Object>> executeSingle();

    /**
     * Executes the query and returns first field value from the first result in
     * the result set.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.) if this
     * method was called at least once.
     *
     * @return Future for the single query result.
     */
    public <T> GridFuture<T> executeSingleField();

    /**
     * Visits every row in fields query result set on every queried node for as long as
     * the visitor predicate returns {@code true}. Once the predicate returns false
     * or all rows in result set have been visited, the visiting process stops.
     * <p>
     * Note that if the passed in grid projection is a local node, then query
     * will be executed locally without distribution to other nodes.
     * <p>
     * Also note that query state cannot be changed (clause, timeout etc.), except
     * arguments, if this method was called at least once.
     *
     * @param vis Visitor predicate.
     * @return Future which will complete whenever visiting on all remote nodes completes or fails.
     */
    public GridFuture<?> visit(GridPredicate<List<Object>> vis);
}
