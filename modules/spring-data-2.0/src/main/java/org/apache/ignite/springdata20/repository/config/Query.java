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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to provide a user defined query for a method.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Query {
    /**
     * Query text string. If not provided, Ignite query generator for Spring Data framework will be used to generate one
     * (only if textQuery = false (default))
     */
    String value() default "";

    /**
     * Whether annotated repository method must use TextQuery search.
     */
    boolean textQuery() default false;

    /**
     * Force SqlFieldsQuery type, deactivating auto-detection based on SELECT statement. Useful for non SELECT
     * statements or to not return hidden fields on SELECT * statements.
     */
    boolean forceFieldsQuery() default false;

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
     */
    boolean collocated() default false;

    /**
     * Query timeout in millis. Sets the query execution timeout. Query will be automatically cancelled if the execution
     * timeout is exceeded. Zero value disables timeout
     *
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     */
    int timeout() default 0;

    /**
     * Sets flag to enforce join order of tables in the query. If set to {@code true} query optimizer will not reorder
     * tables in join. By default is {@code false}.
     * <p>
     * It is not recommended to enable this property until you are sure that your indexes and the query itself are
     * correct and tuned as much as possible but query optimizer still produces wrong join order.
     *
     * <p>
     * Only applicable to SqlFieldsQuery
     */
    boolean enforceJoinOrder() default false;

    /**
     * Specify if distributed joins are enabled for this query.
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     */
    boolean distributedJoins() default false;

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
     */
    boolean lazy() default false;

    /**
     * Sets whether this query should be executed on local node only.
     */
    boolean local() default false;

    /**
     * Sets partitions for a query. The query will be executed only on nodes which are primary for specified
     * partitions.
     * <p>
     * Note what passed array'll be sorted in place for performance reasons, if it wasn't sorted yet.
     * <p>
     * Only applicable to SqlFieldsQuery and SqlQuery
     */
    int[] parts() default {};

    /**
     * Specify whether the annotated method must provide a non null {@link DynamicQueryConfig} parameter with a non
     * empty value (query string) or {@link DynamicQueryConfig#textQuery()} == true.
     * <p>
     * Please, note that  {@link DynamicQueryConfig#textQuery()} annotation parameters will be ignored in favor of those
     * defined in {@link DynamicQueryConfig} parameter if present (runtime ignite query tuning).
     */
    boolean dynamicQuery() default false;

    /**
     * Sets limit to response records count for TextQuery. If 0 or less, considered to be no limit.
     */
    int limit() default 0;

}
