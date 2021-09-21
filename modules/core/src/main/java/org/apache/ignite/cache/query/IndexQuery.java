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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Index query runs over internal index structure and returns cache entries for index rows that match specified criteria.
 */
@IgniteExperimental
public final class IndexQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index query criteria. */
    private List<IndexQueryCriterion> criteria;

    /** Cache Value type. Describes a table within a cache that runs a query. */
    private final String valType;

    /** Index name. If {@code null} then Ignite tries to find an index by {@link #criteria} fields. */
    private final @Nullable String idxName;

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

    /** */
    private void validateAndSetCriteria(List<IndexQueryCriterion> criteria) {
        A.notEmpty(criteria, "criteria");
        A.notNull(criteria.get(0), "criteria");

        Class<?> critCls = criteria.get(0).getClass();

        for (IndexQueryCriterion c: criteria) {
            A.notNull(c, "criteria");
            A.ensure(c.getClass() == critCls,
                "Expect a the same criteria class for merging criteria. Exp=" + critCls + ", act=" + c.getClass());
        }

        this.criteria = Collections.unmodifiableList(criteria);
    }
}
