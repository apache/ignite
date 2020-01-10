/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;

/**
 *
 */
public class QueryCacheImpl implements QueryCache {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** */
    private volatile Map<CacheKey, QueryPlan> cache;

    public QueryCacheImpl() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
    }

    @Override public QueryPlan queryPlan(IgniteCalciteContext ctx, CacheKey key, Function<IgniteCalciteContext, QueryPlan> factory) {
        Map<CacheKey, QueryPlan> cache = this.cache;

        QueryPlan plan = cache.get(key);

        if (plan != null)
            return plan.clone(ctx.createCluster());

        plan = factory.apply(ctx);

        cache.putIfAbsent(key, plan.clone(Commons.EMPTY_CLUSTER));

        return plan;
    }

    /**
     * Clear cached plans.
     */
    public void clear() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
    }
}
