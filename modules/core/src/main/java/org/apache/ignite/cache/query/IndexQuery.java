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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Index queries work over distributed indexes and retrieve cache entries that match the specified criteria.
 * {@code QueryCursor} delivers sorted cache entries by the order defined for queried index.
 *
 * {@code IndexQuery} has to be initialized with cache value class or type. The algorithm of discovering index is as follows:
 * 1. If {@link #idxName} is set, then use it.
 * 2. If {@link #idxName} is not set, then find an index that matches criteria fields.
 * 3. If neither {@link #idxName}, nor {@link #setCriteria(List)} is used, then perform index scan over PK index for specified Value type.
 *
 * Conjuction of items in {@link #criteria} has to represent a valid range to traverse the index tree.
 */
@IgniteExperimental
public final class IndexQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache Value type. Describes a table within a cache that runs a query. */
    private final String valType;

    /** Index name. */
    private final @Nullable String idxName;

    /** Limit */
    private int limit;

    /** Index query criteria. */
    private @Nullable List<IndexQueryCriterion> criteria;

    /** Partition to run IndexQuery over. */
    private @Nullable Integer part;

    /**
     * Specify index with cache value class.
     *
     * @param valCls Cache value class.
     */
    public IndexQuery(Class<?> valCls) {
        this(valCls, null);
    }

    /**
     * Specify index with cache value type.
     *
     * @param valType Cache value type.
     */
    public IndexQuery(String valType) {
        this(valType, null);
    }

    /** Cache entries filter. Applies remotely to a query result cursor. */
    private IgniteBiPredicate<K, V> filter;

    /**
     * Specify index with cache value class and index name.
     *
     * @param valCls Cache value class.
     * @param idxName Index name.
     */
    public IndexQuery(Class<?> valCls, @Nullable String idxName) {
        this(valCls.getName(), idxName);
    }

    /**
     * Specify index with cache value type and index name.
     *
     * @param valType Cache value type.
     * @param idxName Index name.
     */
    public IndexQuery(String valType, @Nullable String idxName) {
        A.notEmpty(valType, "valType");
        A.nullableNotEmpty(idxName, "idxName");

        this.valType = valType;
        this.idxName = idxName;
    }

    /**
     * Sets conjunction (AND) criteria for index query.
     *
     * @param criteria Criteria to set.
     * @return {@code this} for chaining.
     */
    public IndexQuery<K, V> setCriteria(IndexQueryCriterion... criteria) {
        validateAndSetCriteria(Arrays.asList(criteria));

        return this;
    }

    /**
     * Sets conjunction (AND) criteria for index query.
     *
     * @param criteria Criteria to set.
     * @return {@code this} for chaining.
     */
    public IndexQuery<K, V> setCriteria(List<IndexQueryCriterion> criteria) {
        validateAndSetCriteria(new ArrayList<>(criteria));

        return this;
    }

    /**
     * Index query criteria.
     *
     * @return List of criteria for this index query.
     */
    public List<IndexQueryCriterion> getCriteria() {
        return criteria;
    }

    /**
     * Cache Value type.
     *
     * @return Cache Value type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * Index name.
     *
     * @return Index name.
     */
    public String getIndexName() {
        return idxName;
    }

    /**
     * Gets limit to response records count.
     *
     * @return Limit value.
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets limit to response records count.
     *
     * @param limit If 0 or less, considered to be no limit.
     * @return {@code this} For chaining.
     */
    public IndexQuery<K, V> setLimit(int limit) {
        this.limit = limit;

        return this;
    }

    /**
     * Sets remote cache entries filter.
     *
     * @param filter Predicate for remote filtering of query result cursor.
     * @return {@code this} for chaining.
     */
    public IndexQuery<K, V> setFilter(IgniteBiPredicate<K, V> filter) {
        A.notNull(filter, "filter");

        this.filter = filter;

        return this;
    }

    /**
     * Gets remote cache entries filter.
     *
     * @return Filter.
     */
    public IgniteBiPredicate<K, V> getFilter() {
        return filter;
    }

    /**
     * Sets partition number over which this query should iterate. If {@code null}, query will iterate over
     * all partitions in the cache. Must be in the range [0, N) where N is partition number in the cache.
     *
     * @param part Partition number over which this query should iterate.
     * @return {@code this} for chaining.
     */
    public IndexQuery<K, V> setPartition(@Nullable Integer part) {
        A.ensure(part == null || part >= 0,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");

        this.part = part;

        return this;
    }

    /**
     * Gets partition number over which this query should iterate. Will return {@code null} if partition was not
     * set. In this case query will iterate over all partitions in the cache.
     *
     * @return Partition number or {@code null}.
     */
    @Nullable public Integer getPartition() {
        return part;
    }

    /** */
    private void validateAndSetCriteria(List<IndexQueryCriterion> criteria) {
        if (F.isEmpty(criteria))
            return;

        for (IndexQueryCriterion c: criteria)
            A.notNull(c, "criteria");

        this.criteria = Collections.unmodifiableList(criteria);
    }
}
