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
package org.apache.ignite.springdata20.repository.config;

/**
 * Runtime Dynamic query configuration.
 * <p>
 * Can be used as special repository method parameter to provide at runtime:
 * <ol>
 * <li>Dynamic query string (requires {@link Query#dynamicQuery()} == true)
 * <li>Ignite query tuning*
 * </ol>
 * <p>
 * * Please, note that {@link Query} annotation parameters will be ignored in favor of those defined in
 * {@link DynamicQueryConfig} parameter if present.
 *
 * @author Manuel Núñez Sánchez (manuel.nunez@hawkore.com)
 */
public class DynamicQueryConfig {
    /** */
    private String value = "";

    /** */
    private boolean textQuery;

    /** */
    private boolean forceFieldsQry;

    /** */
    private boolean collocated;

    /** */
    private int timeout;

    /** */
    private boolean enforceJoinOrder;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean lazy;

    /** */
    private boolean local;

    /** */
    private int[] parts;

    /** */
    private int limit;

    /**
     * From Query annotation.
     *
     * @param queryConfiguration the query configuration
     * @return the dynamic query config
     */
    public static DynamicQueryConfig fromQueryAnnotation(Query queryConfiguration) {
        DynamicQueryConfig config = new DynamicQueryConfig();
        if (queryConfiguration != null) {
            config.value = queryConfiguration.value();
            config.collocated = queryConfiguration.collocated();
            config.timeout = queryConfiguration.timeout();
            config.enforceJoinOrder = queryConfiguration.enforceJoinOrder();
            config.distributedJoins = queryConfiguration.distributedJoins();
            config.lazy = queryConfiguration.lazy();
            config.parts = queryConfiguration.parts();
            config.local = queryConfiguration.local();
            config.limit = queryConfiguration.limit();
        }
        return config;
    }

    /**
     * Query text string.
     *
     * @return the string
     */
    public String value() {
        return value;
    }

    /**
     * Whether must use TextQuery search.
     *
     * @return the boolean
     */
    public boolean textQuery() {
        return textQuery;
    }

    /**
     * Force SqlFieldsQuery type, deactivating auto-detection based on SELECT statement. Useful for non SELECT
     * statements or to not return hidden fields on SELECT * statements.
     *
     * @return the boolean
     */
    public boolean forceFieldsQuery() {
        return forceFieldsQry;
    }

    /**
     * Sets flag defining if this query is collocated.
     * <p>
     * Collocation flag is used for optimization purposes of queries with GROUP BY statements. Whenever Ignite executes
     * a distributed query, it sends sub-queries to individual cluster members. If you know in advance that the elements
     * of your query selection are collocated together on the same node and you group by collocated key (primary or
     * affinity key), then Ignite can make significant performance and network optimizations by grouping data on remote
     * nodes.
     *
     * <p>
     * Only applicable to SqlFieldsQuery
     *
     * @return the boolean
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * Query timeout in millis. Sets the query execution timeout. Query will be automatically cancelled if the execution
     * timeout is exceeded. Zero value disables timeout
     *
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     *
     * @return the int
     */
    public int timeout() {
        return timeout;
    }

    /**
     * Sets flag to enforce join order of tables in the query. If set to {@code true} query optimizer will not reorder
     * tables in join. By default is {@code false}.
     * <p>
     * It is not recommended to enable this property until you are sure that your indexes and the query itself are
     * correct and tuned as much as possible but query optimizer still produces wrong join order.
     *
     * <p>
     * Only applicable to SqlFieldsQuery
     *
     * @return the boolean
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     *
     * @return the boolean
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * Sets lazy query execution flag.
     * <p>
     * By default Ignite attempts to fetch the whole query result set to memory and send it to the client. For small and
     * medium result sets this provides optimal performance and minimize duration of internal database locks, thus
     * increasing concurrency.
     * <p>
     * If result set is too big to fit in available memory this could lead to excessive GC pauses and even
     * OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing memory
     * consumption at the cost of moderate performance hit.
     * <p>
     * Defaults to {@code false}, meaning that the whole result set is fetched to memory eagerly.
     * <p>
     * Only applicable to SqlFieldsQuery
     *
     * @return the boolean
     */
    public boolean lazy() {
        return lazy;
    }

    /**
     * Sets whether this query should be executed on local node only.
     *
     * @return the boolean
     */
    public boolean local() {
        return local;
    }

    /**
     * Sets partitions for a query. The query will be executed only on nodes which are primary for specified
     * partitions.
     * <p>
     * Note what passed array'll be sorted in place for performance reasons, if it wasn't sorted yet.
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     *
     * @return the int [ ]
     */
    public int[] parts() {
        return parts;
    }

    /**
     * Gets limit to response records count for TextQuery. If 0 or less, considered to be no limit.
     *
     * @return Limit value.
     */
    public int limit() {
        return limit;
    }

    /**
     * Sets value.
     *
     * @param value the value
     * @return this for chaining
     */
    public DynamicQueryConfig setValue(String value) {
        this.value = value;
        return this;
    }

    /**
     * Sets text query.
     *
     * @param textQuery the text query
     * @return this for chaining
     */
    public DynamicQueryConfig setTextQuery(boolean textQuery) {
        this.textQuery = textQuery;
        return this;
    }

    /**
     * Sets force fields query.
     *
     * @param forceFieldsQuery the force fields query
     * @return this for chaining
     */
    public DynamicQueryConfig setForceFieldsQuery(boolean forceFieldsQuery) {
        forceFieldsQry = forceFieldsQuery;
        return this;
    }

    /**
     * Sets collocated.
     *
     * @param collocated the collocated
     * @return this for chaining
     */
    public DynamicQueryConfig setCollocated(boolean collocated) {
        this.collocated = collocated;
        return this;
    }

    /**
     * Sets timeout.
     *
     * @param timeout the timeout
     * @return this for chaining
     */
    public DynamicQueryConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets enforce join order.
     *
     * @param enforceJoinOrder the enforce join order
     * @return this for chaining
     */
    public DynamicQueryConfig setEnforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;
        return this;
    }

    /**
     * Sets distributed joins.
     *
     * @param distributedJoins the distributed joins
     * @return this for chaining
     */
    public DynamicQueryConfig setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
        return this;
    }

    /**
     * Sets lazy.
     *
     * @param lazy the lazy
     * @return this for chaining
     */
    public DynamicQueryConfig setLazy(boolean lazy) {
        this.lazy = lazy;
        return this;
    }

    /**
     * Sets local.
     *
     * @param local the local
     * @return this for chaining
     */
    public DynamicQueryConfig setLocal(boolean local) {
        this.local = local;
        return this;
    }

    /**
     * Sets parts.
     *
     * @param parts the parts
     * @return this for chaining
     */
    public DynamicQueryConfig setParts(int[] parts) {
        this.parts = parts;
        return this;
    }

    /**
     * Sets limit to response records count for TextQuery.
     *
     * @param limit If 0 or less, considered to be no limit.
     * @return {@code this} For chaining.
     */
    public DynamicQueryConfig setLimit(int limit) {
        this.limit = limit;
        return this;
    }
}
