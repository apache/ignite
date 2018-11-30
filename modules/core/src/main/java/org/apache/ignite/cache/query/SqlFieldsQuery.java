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

package org.apache.ignite.cache.query;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * <h1>SQL Fields query.</h1>
 *
 * This kind of query returns specific fields of data based on SQL {@code 'select'} clause,
 * as opposed to {@link SqlQuery}, which always returns the whole key and value objects back.
 * Note that selected fields must be annotated with {@link QuerySqlField} annotation.
 *
 * <h2 class="header">Collocated Flag</h2>
 * Collocation flag is used for optimization purposes. Whenever Ignite executes
 * a distributed query, it sends sub-queries to individual cluster members.
 * If you know in advance that the elements of your query selection are collocated
 * together on the same node, usually based on some <b>affinity-key</b>, Ignite
 * can make significant performance and network optimizations.
 * <p>
 * For example, in case of Word-Count example, we know that all identical words
 * are processed on the same cluster member, because we use the {@code word} itself
 * as affinity key. This allows Ignite to execute the {@code 'limit'} clause on
 * the remote nodes and bring back only the small data set specified within the 'limit' clause,
 * instead of the whole query result as would happen in a non-collocated execution.
 *
 * <h2 class="header">Local flag</h2>
 * When flag is set to {@code True} then results from local node only
 * will be returned.
 * See {@link #setLocal(boolean)}.
 *
 * <h2 class="header">Enforce join order flag</h2>
 * When flag is set to {@code True} then Ignite will not try to optimize query plan with rearranging joins and
 * all joins will be applied in order as query declares.
 * See {@link SqlFieldsQuery#setEnforceJoinOrder(boolean)}
 *
 * <h2 class="header">Cross-Cache Queries</h2>
 * You are allowed to query data from several caches. Cache that this query was created on is
 * treated as default schema in this case. Other caches can be referenced by their names.
 * <p>
 * Note that cache name is case sensitive and has to always be specified in double quotes.
 * Here is an example of cross cache query (note that 'replicated' and 'partitioned' are
 * cache names for replicated and partitioned caches accordingly):
 * <pre name="code" class="java">
 * Query&lt;List&lt;?&gt;&gt; storePurchases = new SqlFieldsQuery(
 *     "Select * from \"replicated\".Store, \"partitioned\".Purchase where Store.id=Purchase.storeId and Store.id=?");
 * </pre>
 *
 * Note: Joins will work correctly only if joined objects are stored in collocated mode or
 * at least one side of the join is stored in {@link org.apache.ignite.cache.CacheMode#REPLICATED} cache or
 * {@link #distributedJoins} is set to {@code True}.
 * See {@link AffinityKey} javadoc for more information about collocation.
 *
 * <h2 class="header">Query usage</h2>
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
 *     ...
 * }
 * </pre>
 * Then you can create and execute queries that check various salary ranges like so:
 * <pre name="code" class="java">
 * Cache&lt;Long, Person&gt; cache = G.grid().cache();
 * ...
 * // Create query which selects salaries based on range for all employees
 * // that work for a certain company.
 * Query&lt;List&lt;?&gt;&gt; qry = new SqlFieldsQuery(
 *     "Select * from Person, Organization where Person.orgId = Organization.id " +
 *         "and Organization.name = ? and Person.salary &gt; ? and Person.salary &lt;= ?");
 *
 * // Query all nodes to find all cached Ignite employees
 * // with salaries less than 1000.
 * cache.query(qry.setArgs("Ignition", 0, 1000)).getAll();
 *
 * <h2 class="header">Custom functions in SQL queries.</h2>
 * It is possible to write custom Java methods and call then form SQL queries. These methods must be public static
 * and annotated with {@link QuerySqlFunction}. Classes containing these methods must be registered in
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])}.
 *
 * <h2 class="header">Geo-Spatial Indexes and Queries</h2>
 * Ignite also support <b>Geo-Spatial Indexes</b>. Here is an example of geo-spatial index:
 * <pre name="code" class="java">
 * private class MapPoint implements Serializable {
 *     // Geospatial index.
 *     &#64;QuerySqlField(index = true)
 *     private org.locationtech.jts.geom.Point location;
 *
 *     // Not indexed field.
 *     &#64;QuerySqlField
 *     private String name;
 *
 *     public MapPoint(org.locationtech.jts.geom.Point location, String name) {
 *         this.location = location;
 *         this.name = name;
 *     }
 * }
 * </pre>
 * Example of spatial query on the geo-indexed field from above:
 * <pre name="code" class="java">
 * org.locationtech.jts.geom.GeometryFactory factory = new org.locationtech.jts.geom.GeometryFactory();
 *
 * org.locationtech.jts.geom.Polygon square = factory.createPolygon(new Coordinate[] {
 *     new org.locationtech.jts.geom.Coordinate(0, 0),
 *     new org.locationtech.jts.geom.Coordinate(0, 100),
 *     new org.locationtech.jts.geom.Coordinate(100, 100),
 *     new org.locationtech.jts.geom.Coordinate(100, 0),
 *     new org.locationtech.jts.geom.Coordinate(0, 0)
 * });
 *
 * Map.Entry<String, UserData> records = cache.query(
 *      new SqlQuery<>(MapPoint.class, "select * from MapPoint where location && ?")
 *          .setArgs(square)
 *      ).getAll();
 * </pre>
 *
 * @see IgniteCache#query(Query)
 */
public class SqlFieldsQuery extends Query<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** SQL Query. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /** Collocation flag. */
    private boolean collocated;

    /** Query timeout in millis. */
    private int timeout;

    /** */
    private boolean enforceJoinOrder;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean replicatedOnly;

    /** */
    private boolean lazy;

    /** Partitions for query */
    private int[] parts;

    /** Schema. */
    private String schema;

    /**
     * Copy constructs SQL fields query.
     *
     * @param qry SQL query.
     */
    public SqlFieldsQuery(SqlFieldsQuery qry) {
        sql = qry.sql;
        args = qry.args;
        collocated = qry.collocated;
        timeout = qry.timeout;
        enforceJoinOrder = qry.enforceJoinOrder;
        distributedJoins = qry.distributedJoins;
        replicatedOnly = qry.replicatedOnly;
        lazy = qry.lazy;
        parts = qry.parts;
        schema = qry.schema;
    }

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     */
    public SqlFieldsQuery(String sql) {
        setSql(sql);
    }

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     * @param collocated Collocated flag.
     */
    public SqlFieldsQuery(String sql, boolean collocated) {
        this.sql = sql;
        this.collocated = collocated;
    }

    /**
     * Gets SQL clause.
     *
     * @return SQL clause.
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets SQL clause.
     *
     * @param sql SQL clause.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setSql(String sql) {
        A.notNull(sql, "sql");

        this.sql = sql;

        return this;
    }

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /**
     * Gets the query execution timeout in milliseconds.
     *
     * @return Timeout value.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets the query execution timeout. Query will be automatically cancelled if the execution timeout is exceeded.
     * @param timeout Timeout value. Zero value disables timeout.
     * @param timeUnit Time unit.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setTimeout(int timeout, TimeUnit timeUnit) {
        this.timeout = QueryUtils.validateTimeout(timeout, timeUnit);

        return this;
    }

    /**
     * Checks if this query is collocated.
     *
     * @return {@code true} If the query is collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * Sets flag defining if this query is collocated.
     *
     * Collocation flag is used for optimization purposes of queries with GROUP BY statements.
     * Whenever Ignite executes a distributed query, it sends sub-queries to individual cluster members.
     * If you know in advance that the elements of your query selection are collocated together on the same node and
     * you group by collocated key (primary or affinity key), then Ignite can make significant performance and network
     * optimizations by grouping data on remote nodes.
     *
     * @param collocated Flag value.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setCollocated(boolean collocated) {
        this.collocated = collocated;

        return this;
    }

    /**
     * Checks if join order of tables if enforced.
     *
     * @return Flag value.
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * Sets flag to enforce join order of tables in the query. If set to {@code true}
     * query optimizer will not reorder tables in join. By default is {@code false}.
     * <p>
     * It is not recommended to enable this property until you are sure that
     * your indexes and the query itself are correct and tuned as much as possible but
     * query optimizer still produces wrong join order.
     *
     * @param enforceJoinOrder Flag value.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setEnforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;

        return this;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * @param distributedJoins Distributed joins enabled.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;

        return this;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joins enabled.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setPageSize(int pageSize) {
        return (SqlFieldsQuery)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setLocal(boolean loc) {
        return (SqlFieldsQuery)super.setLocal(loc);
    }

    /**
     * Specify if the query contains only replicated tables.
     * This is a hint for potentially more effective execution.
     *
     * @param replicatedOnly The query contains only replicated tables.
     * @return {@code this} For chaining.
     * @deprecated No longer used as of Apache Ignite 2.8.
     */
    @Deprecated
    public SqlFieldsQuery setReplicatedOnly(boolean replicatedOnly) {
        this.replicatedOnly = replicatedOnly;

        return this;
    }

    /**
     * Check is the query contains only replicated tables.
     *
     * @return {@code true} If the query contains only replicated tables.
     * @deprecated No longer used as of Apache Ignite 2.8.
     */
    @Deprecated
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * Sets lazy query execution flag.
     * <p>
     * By default Ignite attempts to fetch the whole query result set to memory and send it to the client. For small
     * and medium result sets this provides optimal performance and minimize duration of internal database locks, thus
     * increasing concurrency.
     * <p>
     * If result set is too big to fit in available memory this could lead to excessive GC pauses and even
     * OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing memory
     * consumption at the cost of moderate performance hit.
     * <p>
     * Defaults to {@code false}, meaning that the whole result set is fetched to memory eagerly.
     *
     * @param lazy Lazy query execution flag.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setLazy(boolean lazy) {
        this.lazy = lazy;

        return this;
    }

    /**
     * Gets lazy query execution flag.
     * <p>
     * See {@link #setLazy(boolean)} for more information.
     *
     * @return Lazy flag.
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * Gets partitions for query, in ascending order.
     */
    @Nullable public int[] getPartitions() {
        return parts;
    }

    /**
     * Sets partitions for a query.
     * The query will be executed only on nodes which are primary for specified partitions.
     * <p>
     * Note what passed array'll be sorted in place for performance reasons, if it wasn't sorted yet.
     *
     * @param parts Partitions.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setPartitions(@Nullable int... parts) {
        this.parts = prepare(parts);

        return this;
    }

    /**
     * Get schema for the query.
     * If not set, current cache name is used, which means you can
     * omit schema name for tables within the current cache.
     *
     * @return Schema. Null if schema is not set.
     */
    @Nullable public String getSchema() {
        return schema;
    }

    /**
     * Set schema for the query.
     * If not set, current cache name is used, which means you can
     * omit schema name for tables within the current cache.
     *
     * @param schema Schema. Null to unset schema.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setSchema(@Nullable String schema) {
        this.schema = schema;

        return this;
    }

    /**
     * @return Copy of this query.
     */
    public SqlFieldsQuery copy() {
        return new SqlFieldsQuery(this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlFieldsQuery.class, this);
    }
}