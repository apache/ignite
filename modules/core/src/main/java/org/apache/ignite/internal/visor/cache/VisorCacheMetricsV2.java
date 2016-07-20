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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;

/**
 * Data transfer object for {@link CacheMetrics}.
 */
public class VisorCacheMetricsV2 extends VisorCacheMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Memory size allocated in off-heap. */
    private long offHeapAllocatedSize;

    /** Number of cache entries stored in off-heap memory. */
    private long offHeapEntriesCount;

    /** {@inheritDoc} */
    @Override
    public VisorCacheMetrics from(IgniteEx ignite, String cacheName) {
        super.from(ignite, cacheName);

        GridCacheProcessor cacheProcessor = ignite.context().cache();

        GridCacheAdapter<Object, Object> c = cacheProcessor.internalCache(cacheName);

        offHeapAllocatedSize = c.offHeapAllocatedSize();
        offHeapEntriesCount = c.offHeapEntriesCount();

        return this;
    }

    /**
     * @return Memory size allocated in off-heap.
     */
    public long offHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /**
     * @return Number of cache entries stored in off-heap memory.
     */
    public long offHeapEntriesCount() {
        return offHeapEntriesCount;
    }
}
