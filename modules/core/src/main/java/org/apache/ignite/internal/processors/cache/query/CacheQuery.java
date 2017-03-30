/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Main API for configuring and executing cache queries.
 * <p>
 * <h1 class="header">SQL Queries</h1>
 * {@code SQL} query allows to execute distributed cache
 * queries using standard SQL syntax. All values participating in where clauses
 * or joins must be annotated with {@link QuerySqlField} annotation.
 * <h2 class="header">Field Queries</h2>
 * By default {@code select} clause is ignored as query result contains full objects.
 * This type of query replaces full objects with individual fields. Note that selected fields
 * must be annotated with {@link QuerySqlField} annotation.
 * <h2 class="header">Cross-Cache Queries</h2>
 * You are allowed to query data from several caches. Cache that this query was created on is
 * treated as default schema in this case. Other caches can be referenced by their names.
 * <p>
 * Note that cache name is case sensitive and has to always be specified in double quotes.
 * Here is an example of cross cache query (note that 'replicated' and 'partitioned' are
 * cache names for replicated and partitioned caches accordingly):
 * <pre name="code" class="java">
 * CacheQuery&lt;Map.Entry&lt;Integer, FactPurchase&gt;&gt; storePurchases = cache.queries().createSqlQuery(
 *     Purchase.class,
 *     "from \"replicated\".Store, \"partitioned\".Purchase where Store.id=Purchase.storeId and Store.id=?");
 * </pre>
 * <h2 class="header">Custom functions in SQL queries.</h2>
 * It is possible to write custom Java methods and call then form SQL queries. These methods must be public static
 * and annotated with {@link QuerySqlFunction}. Classes containing these methods must be registered in
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])}.
 * <h1 class="header">Full Text Queries</h1>
 * Ignite supports full text queries based on Apache Lucene engine. Note that all fields that
 * are expected to show up in text query results must be annotated with {@link QueryTextField}
 * annotation.
 * <h1 class="header">Scan Queries</h1>
 * Sometimes when it is known in advance that SQL query will cause a full data scan, or whenever data set
 * is relatively small, the full scan query may be used. This query will iterate over all cache
 * entries, skipping over entries that don't pass the optionally provided key-value filter.
 * <h2 class="header">Limitations</h2>
 * Data in Ignite cache is usually distributed across several nodes,
 * so some queries may not work as expected. Keep in mind following limitations
 * (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         {@code Group by} and {@code sort by} statements are applied separately
 *         on each node, so result set will likely be incorrectly grouped or sorted
 *         after results from multiple remote nodes are grouped together.
 *     </li>
 *     <li>
 *         Aggregation functions like {@code sum}, {@code max}, {@code avg}, etc.
 *         are also applied on each node. Therefore you will get several results
 *         containing aggregated values, one for each node.
 *     </li>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         colocated mode or at least one side of the join is stored in
 *         {@link org.apache.ignite.cache.CacheMode#REPLICATED} cache. Refer to
 *         {@link AffinityKey} javadoc for more information about colocation.
 *     </li>
 * </ul>
 * <h1 class="header">Query usage</h1>
 * As an example, suppose we have data model consisting of {@code 'Employee'} and {@code 'Organization'}
 * classes defined as follows:
 * <pre name="code" class="java">
 * public class Organization {
 *     // Indexed field.
 *     &#64;QuerySqlField(index = true)
 *     private long id;
 *
 *     // Indexed field.
 *     &#64;QuerySqlField(index = true)
 *     private String name;
 *     ...
 * }
 *
 * public class Person {
 *     // Indexed field.
 *     &#64;QuerySqlField(index = true)
 *     private long id;
 *
 *     // Indexed field (Organization ID, used as a foreign key).
 *     &#64;QuerySqlField(index = true)
 *     private long orgId;
 *
 *     // Without SQL field annotation, this field cannot be used in queries.
 *     private String name;
 *
 *     // Not indexed field.
 *     &#64;QuerySqlField
 *     private double salary;
 *
 *     // Index for text search.
 *     &#64;QueryTextField
 *     private String resume;
 *     ...
 * }
 * </pre>
 * Then you can create and execute queries that check various salary ranges like so:
 * <pre name="code" class="java">
 * Cache&lt;Long, Person&gt; cache = G.grid().cache();
 * ...
 * // Create query which selects salaries based on range for all employees
 * // that work for a certain company.
 * CacheQuery&lt;Map.Entry&lt;Long, Person&gt;&gt; qry = cache.queries().createSqlQuery(Person.class,
 *     "from Person, Organization where Person.orgId = Organization.id " +
 *         "and Organization.name = ? and Person.salary &gt; ? and Person.salary &lt;= ?");
 *
 * // Query all nodes to find all cached Ignite employees
 * // with salaries less than 1000.
 * qry.execute("Ignition", 0, 1000);
 *
 * // Query only remote nodes to find all remotely cached Ignite employees
 * // with salaries greater than 1000 and less than 2000.
 * qry.projection(grid.remoteProjection()).execute("Ignition", 1000, 2000);
 * </pre>
 * Here is a possible query that will use Lucene text search to scan all resumes to
 * check if employees have {@code Master} degree:
 * <pre name="code" class="java">
 * CacheQuery&lt;Map.Entry&lt;Long, Person&gt;&gt; mastersQry =
 *     cache.queries().createFullTextQuery(Person.class, "Master");
 *
 * // Query all cache nodes.
 * mastersQry.execute();
 * </pre>
 * <h1 class="header">Geo-Spatial Indexes and Queries</h1>
 * Ignite also support <b>Geo-Spatial Indexes</b>. Here is an example of geo-spatial index:
 * <pre name="code" class="java">
 * private class MapPoint implements Serializable {
 *     // Geospatial index.
 *     &#64;QuerySqlField(index = true)
 *     private com.vividsolutions.jts.geom.Point location;
 *
 *     // Not indexed field.
 *     &#64;QuerySqlField
 *     private String name;
 *
 *     public MapPoint(com.vividsolutions.jts.geom.Point location, String name) {
 *         this.location = location;
 *         this.name = name;
 *     }
 * }
 * </pre>
 * Example of spatial query on the geo-indexed field from above:
 * <pre name="code" class="java">
 * com.vividsolutions.jts.geom.GeometryFactory factory = new com.vividsolutions.jts.geom.GeometryFactory();
 *
 * com.vividsolutions.jts.geom.Polygon square = factory.createPolygon(new Coordinate[] {
 *     new com.vividsolutions.jts.geom.Coordinate(0, 0),
 *     new com.vividsolutions.jts.geom.Coordinate(0, 100),
 *     new com.vividsolutions.jts.geom.Coordinate(100, 100),
 *     new com.vividsolutions.jts.geom.Coordinate(100, 0),
 *     new com.vividsolutions.jts.geom.Coordinate(0, 0)
 * });
 *
 * Map.Entry<String, UserData> records = cache.queries().createSqlQuery(MapPoint.class, "select * from MapPoint where location && ?")
 *     .queryArguments(square)
 *     .execute()
 *     .get();
 * </pre>
 */
public interface CacheQuery<T> {
    /**
     * Sets result page size. If not provided, {@link Query#DFLT_PAGE_SIZE} will be used.
     * Results are returned from queried nodes one page at a tme.
     *
     * @param pageSize Page size.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> pageSize(int pageSize);

    /**
     * Sets query timeout. {@code 0} means there is no timeout (this
     * is a default value).
     *
     * @param timeout Query timeout.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> timeout(long timeout);

    /**
     * Sets whether or not to keep all query results local. If not - only the current page
     * is kept locally. Default value is {@code true}.
     *
     * @param keepAll Keep results or not.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> keepAll(boolean keepAll);

    /**
     * Sets whether or not to include backup entries into query result. This flag
     * is {@code false} by default.
     *
     * @param incBackups Query {@code includeBackups} flag.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> includeBackups(boolean incBackups);

    /**
     * Sets whether or not to deduplicate query result set. If this flag is {@code true}
     * then query result will not contain some key more than once even if several nodes
     * returned entries with the same keys. Default value is {@code false}.
     *
     * @param dedup Query {@code enableDedup} flag.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> enableDedup(boolean dedup);

    /**
     * Sets optional grid projection to execute this query on.
     *
     * @param prj Projection.
     * @return {@code this} query instance for chaining.
     */
    public CacheQuery<T> projection(ClusterGroup prj);

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
     * @param args Optional arguments.
     * @return Future for the query result.
     */
    public CacheQueryFuture<T> execute(@Nullable Object... args);

    /**
     * Executes the query the same way as {@link #execute(Object...)} method but reduces result remotely.
     *
     * @param rmtReducer Remote reducer.
     * @param args Optional arguments.
     * @return Future for the query result.
     */
    public <R> CacheQueryFuture<R> execute(IgniteReducer<T, R> rmtReducer, @Nullable Object... args);

    /**
     * Gets metrics for this query.
     *
     * @return Query metrics.
     */
    public QueryMetrics metrics();

    /**
     * Resets metrics for this query.
     */
    public void resetMetrics();

    /**
     * @return Scan query iterator.
     */
    public GridCloseableIterator executeScanQuery() throws IgniteCheckedException;
}
