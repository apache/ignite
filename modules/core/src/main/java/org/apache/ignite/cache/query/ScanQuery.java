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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

/**
 * Scan query over cache entries. Will accept all the entries if no predicate was set.
 *
 * @see IgniteCache#query(Query)
 * @see IgniteCache#localQuery(Query)
 */
public class ScanQuery<K, V> extends Query<ScanQuery<K, V>> {
    /** */
    private IgniteBiPredicate<K,V> filter;

    /**
     * Create scan query returning all entries.
     */
    public ScanQuery() {
        this(null);
    }

    /**
     * Create scan query with filter.
     *
     * @param filter Filter. If {@code null} then all entries will be returned.
     */
    public ScanQuery(@Nullable IgniteBiPredicate<K,V> filter) {
        setFilter(filter);
    }

    /**
     * Gets filter.
     *
     * @return Filter.
     */
    public IgniteBiPredicate<K,V> getFilter() {
        return filter;
    }

    /**
     * Sets filter.
     *
     * @param filter Filter. If {@code null} then all entries will be returned.
     */
    public void setFilter(@Nullable IgniteBiPredicate<K,V> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ScanQuery.class, this);
    }
}