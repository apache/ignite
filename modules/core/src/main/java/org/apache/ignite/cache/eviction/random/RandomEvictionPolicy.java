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

package org.apache.ignite.cache.eviction.random;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_SIZE;

/**
 * Cache eviction policy which will select random cache entry for eviction if cache
 * size exceeds the {@link #getMaxSize()} parameter. This implementation is
 * extremely light weight, lock-free, and does not create any data structures to maintain
 * any order for eviction.
 * <p>
 * Random eviction will provide the best performance over any key queue in which every
 * key has the same probability of being accessed.
 */
public class RandomEvictionPolicy<K, V> implements EvictionPolicy<K, V>, RandomEvictionPolicyMBean, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum size. */
    private volatile int max = DFLT_CACHE_SIZE;

    /**
     * Constructs random eviction policy with all defaults.
     */
    public RandomEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructs random eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public RandomEvictionPolicy(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
        if (!entry.isCached())
            return;

        IgniteCache<K, V> cache = entry.unwrap(IgniteCache.class);

        int size = cache.localSize(CachePeekMode.ONHEAP);

        for (int i = max; i < size; i++) {
            Cache.Entry<K, V> e = cache.randomEntry();

            if (e != null)
                e.unwrap(EvictableEntry.class).evict();
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(max);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        max = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RandomEvictionPolicy.class, this);
    }
}