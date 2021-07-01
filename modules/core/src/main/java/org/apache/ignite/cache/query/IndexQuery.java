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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    /** Cache Value class. Describes a table within a cache that runs a query. */
    private final String valCls;

    /** Optional index name. */
    private final @Nullable String idxName;

    /**
     * Specify index with cache value class.
     *
     * @param valCls Cache value class.
     */
    public IndexQuery(Class<V> valCls) {
        this(valCls, null);
    }

    /**
     * Specify index with cache value class and index name. If {@code idxName} is {@code null} then Ignite checks
     * all indexes to find best match by {@link #valCls} and {@link #criteria} fields.
     *
     * @param valCls Cache value class.
     * @param idxName Optional Index name.
     */
    public IndexQuery(Class<V> valCls, @Nullable String idxName) {
        A.notNull(valCls, "valCls");

        if (idxName != null)
            A.notNullOrEmpty(idxName, "idxName");

        this.valCls = valCls.getName();
        this.idxName = idxName;
    }

    /**
     * Provide multiple index query criterion joint with AND.
     */
    public IndexQuery<K, V> setCriteria(IndexQueryCriterion criterion, IndexQueryCriterion... criteria) {
        List<IndexQueryCriterion> cc = new ArrayList<>();

        cc.add(criterion);
        cc.addAll(Arrays.asList(criteria));

        validateAndSetCriteria(cc);

        return this;
    }

    /**
     * Provide multiple index query criterion joint with AND.
     */
    public IndexQuery<K, V> setCriteria(List<IndexQueryCriterion> criteria) {
        validateAndSetCriteria(new ArrayList<>(criteria));

        return this;
    }

    /** Index query criteria. */
    public List<IndexQueryCriterion> getCriteria() {
        return criteria;
    }

    /** Cache value class. */
    public String getValueClass() {
        return valCls;
    }

    /** Index name. */
    public @Nullable String getIndexName() {
        return idxName;
    }

    /** */
    private void validateAndSetCriteria(List<IndexQueryCriterion> criteria) {
        A.notEmpty(criteria, "criteria");
        A.notNull(criteria.get(0), "criteria");

        Class<?> critCls = criteria.get(0).getClass();

        Set<String> fields = new HashSet<>();

        for (IndexQueryCriterion c: criteria) {
            A.notNull(c, "criteria");
            A.ensure(c.getClass() == critCls,
                "Expect a the same criteria class for merging criteria. Exp=" + critCls + ", act=" + c.getClass());

            A.ensure(!fields.contains(c.field()), "Duplicated field in criteria: " + c.field() + ".");

            fields.add(c.field());
        }

        this.criteria = Collections.unmodifiableList(criteria);
    }
}
