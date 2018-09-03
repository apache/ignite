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

package org.apache.ignite.cache.eviction;

import javax.cache.configuration.Factory;
import org.apache.ignite.internal.util.typedef.internal.A;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_SIZE;

/**
 * Common functionality implementation for eviction policies factories.
 */
public abstract class AbstractEvictionPolicyFactory<T> implements Factory<T> {
    /** */
    private int maxSize = DFLT_CACHE_SIZE;

    /** */
    private int batchSize = 1;

    /** */
    private long maxMemSize;

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicyFactory setMaxSize(int max) {
        A.ensure(max >= 0, "max >= 0");

        this.maxSize = max;

        return this;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Sets batch size.
     *
     * @param batchSize Batch size.
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicyFactory setBatchSize(int batchSize) {
        A.ensure(batchSize > 0, "batchSize > 0");

        this.batchSize = batchSize;

        return this;
    }

    /**
     * Gets batch size.
     *
     * @return batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets maximum allowed cache size in bytes.
     *
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicyFactory setMaxMemorySize(long maxMemSize) {
        A.ensure(maxMemSize >= 0, "maxMemSize >= 0");

        this.maxMemSize = maxMemSize;

        return this;
    }

    /**
     * Gets maximum allowed cache size in bytes.
     *
     * @return maximum allowed cache size in bytes.
     */
    public long getMaxMemorySize() {
        return maxMemSize;
    }

}
