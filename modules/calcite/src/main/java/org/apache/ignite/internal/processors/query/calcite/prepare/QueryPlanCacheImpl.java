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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link QueryPlanCache} that simply wraps a {@link Caffeine} cache.
 */
public class QueryPlanCacheImpl implements QueryPlanCache {
    private final ConcurrentMap<CacheKey, QueryPlan> cache;

    /**
     * Creates a plan cache of provided size.
     *
     * @param cacheSize Desired cache size.
     */
    public QueryPlanCacheImpl(int cacheSize) {
        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, QueryPlan>build()
                .asMap();
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan queryPlan(PlanningContext ctx, CacheKey key, QueryPlanFactory factory) {
        Map<CacheKey, QueryPlan> cache = this.cache;
        QueryPlan plan = cache.computeIfAbsent(key, k -> factory.create(ctx));

        return plan.copy();
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan queryPlan(CacheKey key) {
        Map<CacheKey, QueryPlan> cache = this.cache;
        QueryPlan plan = cache.get(key);

        return plan != null ? plan.copy() : null;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        clear();
    }
}
