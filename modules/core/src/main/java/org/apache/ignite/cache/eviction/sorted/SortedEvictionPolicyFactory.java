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

import org.apache.ignite.cache.eviction.AbstractEvictionPolicyFactory;

/**
 * Factory class for {@link SortedEvictionPolicy}.
 */
public class SortedEvictionPolicyFactory<K,V> extends AbstractEvictionPolicyFactory<SortedEvictionPolicy<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

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


    /** {@inheritDoc} */
    @Override public SortedEvictionPolicy<K, V> create() {
        SortedEvictionPolicy<K, V> policy = new SortedEvictionPolicy<>();

        policy.setBatchSize(getBatchSize());
        policy.setMaxMemorySize(getMaxMemorySize());
        policy.setMaxSize(getMaxSize());

        return policy;
    }

}
