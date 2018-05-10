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

package org.apache.ignite.cache.eviction.sorted;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.ignite.cache.eviction.AbstractEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.EvictableEntry;

/**
 * Factory class for {@link SortedEvictionPolicy}.
 *
 * Creates cache Eviction policy which will select the minimum cache entry for eviction.
 * <p>
 * The eviction starts in the following cases:
 * <ul>
 *     <li>The cache size becomes {@code batchSize} elements greater than the maximum size.</li>
 *     <li>
 *         The size of cache entries in bytes becomes greater than the maximum memory size.
 *         The size of cache entry calculates as sum of key size and value size.
 *     </li>
 * </ul>
 * <b>Note:</b>Batch eviction is enabled only if maximum memory limit isn't set ({@code maxMemSize == 0}).
 * {@code batchSize} elements will be evicted in this case. The default {@code batchSize} value is {@code 1}.
 * <p>
 * Entries comparison based on {@link Comparator} instance if provided.
 * Default {@code Comparator} behaviour is use cache entries keys for comparison that imposes a requirement for keys
 * to implement {@link Comparable} interface.
 * <p>
 * User defined comparator should implement {@link Serializable} interface.
 */
public class SortedEvictionPolicyFactory<K,V> extends AbstractEvictionPolicyFactory<SortedEvictionPolicy<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Comparator. */
    private Comparator<EvictableEntry<K, V>> comp;

    /** */
    public SortedEvictionPolicyFactory() {
    }

    /** */
    public SortedEvictionPolicyFactory(int maxSize) {
        setMaxSize(maxSize);
    }

    /** */
    public SortedEvictionPolicyFactory(int maxSize, int batchSize, long maxMemSize) {
        setMaxSize(maxSize);
        setBatchSize(batchSize);
        setMaxMemorySize(maxMemSize);
    }

    /**
     * Gets entries comparator.
     * @return entry comparator.
     */
    public Comparator<EvictableEntry<K, V>> getComp() {
        return comp;
    }

    /**
     * Sets entries comparator.
     *
     * @param comp entry comparator.
     */
    public void setComp(Comparator<EvictableEntry<K, V>> comp) {
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override public SortedEvictionPolicy<K, V> create() {
        SortedEvictionPolicy<K, V> policy = new SortedEvictionPolicy<>(comp);

        policy.setBatchSize(getBatchSize());
        policy.setMaxMemorySize(getMaxMemorySize());
        policy.setMaxSize(getMaxSize());

        return policy;
    }

}
