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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

/**
 * Scan query over cache entries. Will accept all the entries if no predicate was set.
 */
public class QueryScan<K, V> extends Query<QueryScan<K, V>> {
    /** */
    private IgniteBiPredicate<K,V> filter;

    /**
     * Create scan query returning all entries.
     */
    public QueryScan() {
        this(new IgniteBiPredicate<K,V>() {
            @Override public boolean apply(K k, V v) {
                return true;
            }
        });
    }

    /**
     * Create scan query with filter.
     *
     * @param filter Filter.
     */
    public QueryScan(IgniteBiPredicate<K,V> filter) {
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
     * @param filter Filter.
     */
    public void setFilter(IgniteBiPredicate<K,V> filter) {
        A.notNull(filter, "filter");

        this.filter = filter;
    }
}