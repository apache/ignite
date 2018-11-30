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

import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * <h1 class="header">SQL Query</h1>
 * {@code SQL} query allows to execute distributed cache queries using standard SQL syntax.
 * By default {@code select} clause is ignored as query result contains full objects.
 *
 * All values participating in where clauses or joins must be annotated with {@link QuerySqlField} annotation.
 *
 * <h2 class="header">Custom functions in SQL queries.</h2>
 * It is possible to write custom Java methods and call then form SQL queries. These methods must be public static
 * and annotated with {@link QuerySqlFunction}. Classes containing these methods must be registered in
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])}.
 *
 * <h2 class="header">Query usage:</h2>
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
 *     ...
 * }
 * </pre>
 * Then you can create and execute queries that check various salary ranges like so:
 * <pre name="code" class="java">
 * Cache&lt;Long, Person&gt; cache = G.grid().cache();
 * ...
 * // Create query which selects salaries based on range for all employees
 * // that work for a certain company.
 * Query&lt;Cache.Entry&lt;Long, Person&gt;&gt; qry = new SqlQuery<>(Person.class,
 *     "from Person, Organization where Person.orgId = Organization.id " +
 *         "and Organization.name = ? and Person.salary &gt; ? and Person.salary &lt;= ?");
 *
 * // Query all nodes to find all cached Ignite employees
 * // with salaries less than 1000.
 * cache.query(qry.setArgs("Ignition", 0, 1000)).getAll();
 *
 * // Query only local node to find all locally cached Ignite employees
 * // with salaries greater than 1000 and less than 2000.
 * cache.query(qry.setLocal(true).setArgs("Ignition", 0, 1000)).getAll();
 * </pre>
 *
 * <h2 class="header">Cross-Cache Queries</h2>
 * You are allowed to query data from several caches. Cache that this query was created on is
 * treated as default schema in this case. Other caches can be referenced by their names.
 * <p>
 * Note that cache name is case sensitive and has to always be specified in double quotes.
 * Here is an example of cross cache query (note that 'replicated' and 'partitioned' are
 * cache names for replicated and partitioned caches accordingly):
 * <pre name="code" class="java">
 * Query&lt;Cache.Entry&lt;Integer, FactPurchase&gt;&gt; storePurchases = new SqlQuery(
 *     Purchase.class,
 *     "from \"replicated\".Store, \"partitioned\".Purchase where Store.id=Purchase.storeId and Store.id=?");
 * </pre>
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
 * Cache.Entry<String, UserData> records = cache.query(
 *      new SqlQuery<>(MapPoint.class, "select * from MapPoint where location && ?")
 *          .setArgs(square)
 *      ).getAll();
 * </pre>
 *
 * @see IgniteCache#query(Query)
 */
public final class SqlQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String type;

    /** Table alias */
    private String alias;

    /** SQL clause. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /** Timeout in millis. */
    private int timeout;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean replicatedOnly;

    /** Partitions for query */
    private int[] parts;

    /**
     * Constructs query for the given type name and SQL query.
     *
     * @param type Type.
     * @param sql SQL Query.
     */
    public SqlQuery(String type, String sql) {
        setType(type);
        setSql(sql);
    }

    /**
     * Constructs query for the given type and SQL query.
     *
     * @param type Type.
     * @param sql SQL Query.
     */
    public SqlQuery(Class<?> type, String sql) {
        setType(type);
        setSql(sql);
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
    public SqlQuery<K, V> setSql(String sql) {
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
    public SqlQuery<K, V> setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /**
     * Gets type for query.
     *
     * @return Type.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setType(String type) {
        this.type = type;

        return this;
    }

    /**
     * Sets table alias for type.
     *
     * @return Table alias.
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Gets table alias for type.
     *
     * @param alias table alias for type that is used in query.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setAlias(String alias) {
        this.alias = alias;

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
     *
     * @param timeout Timeout value. Zero value disables timeout.
     * @param timeUnit Time granularity.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setTimeout(int timeout, TimeUnit timeUnit) {
        this.timeout = QueryUtils.validateTimeout(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlQuery<K, V> setPageSize(int pageSize) {
        return (SqlQuery<K, V>)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public SqlQuery<K, V> setLocal(boolean loc) {
        return (SqlQuery<K, V>)super.setLocal(loc);
    }

    /**
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setType(Class<?> type) {
        return setType(QueryUtils.typeName(type));
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * When disabled, join results will only contain colocated data (joins work locally).
     * When enabled, joins work as expected, no matter how the data is distributed.
     *
     * @param distributedJoins Distributed joins enabled.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setDistributedJoins(boolean distributedJoins) {
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

    /**
     * Specify if the query contains only replicated tables.
     * This is a hint for potentially more effective execution.
     *
     * @param replicatedOnly The query contains only replicated tables.
     * @return {@code this} For chaining.
     * @deprecated No longer used as of Apache Ignite 2.8.
     */
    @Deprecated
    public SqlQuery<K, V> setReplicatedOnly(boolean replicatedOnly) {
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
    public SqlQuery setPartitions(@Nullable int... parts) {
        this.parts = prepare(parts);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQuery.class, this);
    }
}