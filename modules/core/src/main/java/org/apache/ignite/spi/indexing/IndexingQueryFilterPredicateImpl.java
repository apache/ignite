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

package org.apache.ignite.spi.indexing;

import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;

import java.util.Set;

/**
 * Indexing query filter predicate.
 */
public class IndexingQueryFilterPredicateImpl<K, V> implements IndexingQueryFilterPredicate<K, V> {
    /** Affinity manager. */
    private final GridCacheAffinityManager aff;

    /** Partitions. */
    private final Set<Integer> parts;

    /**
     * Constructor.
     *
     * @param aff Affinity.
     * @param parts Partitions.
     */
    public IndexingQueryFilterPredicateImpl(GridCacheAffinityManager aff, Set<Integer> parts) {
        this.aff = aff;
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(K key, V val) {
        int part = aff.partition(key);

        return parts.contains(part);
    }
}
