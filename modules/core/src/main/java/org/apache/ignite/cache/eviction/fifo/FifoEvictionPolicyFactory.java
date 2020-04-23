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

package org.apache.ignite.cache.eviction.fifo;

import org.apache.ignite.cache.eviction.AbstractEvictionPolicyFactory;

/**
 * Factory class for {@link FifoEvictionPolicy}.
 *
 * Creates cache Eviction policy based on {@code First In First Out (FIFO)} algorithm and supports batch eviction.
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
 * {@link FifoEvictionPolicy} implementation is very efficient since it does not create any additional
 * table-like data structures. The {@code FIFO} ordering information is
 * maintained by attaching ordering metadata to cache entries.
 */
public class FifoEvictionPolicyFactory<K, V> extends AbstractEvictionPolicyFactory<FifoEvictionPolicy<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Constructor. */
    public FifoEvictionPolicyFactory() {
    }

    /** Constructor. */
    public FifoEvictionPolicyFactory(int maxSize) {
        setMaxSize(maxSize);
    }

    /** */
    public FifoEvictionPolicyFactory(int maxSize, int batchSize, long maxMemSize) {
        setMaxSize(maxSize);
        setBatchSize(batchSize);
        setMaxMemorySize(maxMemSize);
    }

    /** {@inheritDoc} */
    @Override public FifoEvictionPolicy<K, V> create() {
        FifoEvictionPolicy<K, V> policy = new FifoEvictionPolicy<>();

        policy.setBatchSize(getBatchSize());
        policy.setMaxMemorySize(getMaxMemorySize());
        policy.setMaxSize(getMaxSize());

        return policy;
    }
}
