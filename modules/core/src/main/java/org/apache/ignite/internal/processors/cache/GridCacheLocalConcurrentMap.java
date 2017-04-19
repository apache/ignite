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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * GridCacheConcurrentMap implementation for local and near caches.
 */
public class GridCacheLocalConcurrentMap extends GridCacheConcurrentMapImpl {
    /** */
    private final AtomicInteger pubSize = new AtomicInteger();

    public GridCacheLocalConcurrentMap(GridCacheContext ctx,
        GridCacheMapEntryFactory factory, int initialCapacity) {
        super(ctx, factory, initialCapacity);
    }

    public GridCacheLocalConcurrentMap(GridCacheContext ctx,
        GridCacheMapEntryFactory factory, int initialCapacity, float loadFactor, int concurrencyLevel) {
        super(ctx, factory, initialCapacity, loadFactor, concurrencyLevel);
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        return pubSize.get();
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        pubSize.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        pubSize.decrementAndGet();
    }
}
