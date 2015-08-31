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

package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;

/**
 * Annotates fields for SQL queries. All fields that will be involved in SQL clauses must have
 * this annotation. For more information about cache queries see {@link CacheQuery} documentation.
 * @see CacheQuery
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface QuerySqlField {
    /**
     * Specifies whether cache should maintain an index for this field or not.
     * Just like with databases, field indexing may require additional overhead
     * during updates, but makes select operations faster.
     * <p>
     * When indexing SPI and indexed field is
     * of type {@code com.vividsolutions.jts.geom.Geometry} (or any subclass of this class) then Ignite will
     * consider this index as spatial providing performance boost for spatial queries.
     *
     * @return {@code True} if index must be created for this field in database.
     */
    boolean index() default false;

    /**
     * Specifies whether index should be in descending order or not. This property only
     * makes sense if {@link #index()} property is set to {@code true}.
     *
     * @return {@code True} if field index should be in descending order.
     */
    boolean descending() default false;

    /**
     * Array of index groups this field belongs to. Groups are used for compound indexes,
     * whenever index should be created on more than one field. All fields within the same
     * group will belong to the same index.
     * <p>
     * Group indexes are needed because SQL engine can utilize only one index per table occurrence in a query.
     * For example if we have two separate indexes on fields {@code a} and {@code b} of type {@code X} then
     * query {@code select * from X where a = ? and b = ?} will use for filtering either index on field {@code a}
     * or {@code b} but not both. For more effective query execution here it is preferable to have a single
     * group index on both fields.
     * <p>
     * For more complex scenarios please refer to {@link QuerySqlField.Group} documentation.
     *
     * @return Array of group names.
     */
    String[] groups() default {};

    /**
     * Array of ordered index groups this field belongs to. For more information please refer to
     * {@linkplain QuerySqlField.Group} documentation.
     *
     * @return Array of ordered group indexes.
     * @see #groups()
     */
    Group[] orderedGroups() default {};

    /**
     * Property name. If not provided then field name will be used.
     *
     * @return Name of property.
     */
    String name() default "";

    /**
     * Describes group of index and position of field in this group.
     * <p>
     * Opposite to {@link #groups()} this annotation gives control over order of fields in a group index.
     * This can be needed in scenarios when we have a query like
     * {@code select * from X where a = ? and b = ? order by b desc}. If we have index {@code (a asc, b asc)}
     * sorting on {@code b} will be performed. Here it is preferable to have index {@code (b desc, a asc)}
     * which will still allow query to search on index using both fields and avoid sorting because index
     * is already sorted in needed way.
     *
     * @see #groups()
     * @see #orderedGroups()
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    @SuppressWarnings("PublicInnerClass")
    public static @interface Group {
        /**
         * Group index name where this field participate.
         *
         * @return Group index name
         */
        String name();

        /**
         * Fields in this group index will be sorted on this attribute.
         *
         * @return Order number.
         */
        int order();

        /**
         * Defines sorting order for this field in group.
         *
         * @return True if field will be in descending order.
         */
        boolean descending() default false;
    }
}