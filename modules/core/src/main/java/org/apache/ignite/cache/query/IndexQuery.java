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

import javax.cache.Cache;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Index query runs over internal index structure and returns cache entries for index rows that match specified criteria.
 */
@IgniteExperimental
public class IndexQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index query criteria describes index query clause. */
    private IndexQueryCriteria criteria;

    /** Cache Value class. Describes a table within a cache that runs a query. */
    private final String valCls;

    /** Optional index name. Find index by fields in {@link #criteria}. */
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
     * all indexes to find best match by {@link #valCls} and {@link IndexQueryCriteria#fields()}.
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
     * Provide multiple index query criteria joint with AND.
     */
    public IndexQuery<K, V> setCriteria(IndexQueryCriteria criterion, IndexQueryCriteria... criteria) {
        A.notNull(criterion, "criterion");

        this.criteria = criterion;

        for (IndexQueryCriteria c: criteria) {
            A.notNull(c, "criteria");

            this.criteria.and(c);
        }

        return this;
    }

    /** Index query criteria. */
    public IndexQueryCriteria getIndexCriteria() {
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
}
