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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;

/**
 *
 */
public class QueryCacheImpl extends AbstractService implements QueryCache, SchemaChangeListener {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** */
    private volatile Map<CacheKey, QueryPlan> cache;

    /**
     * @param ctx Kernal context.
     */
    public QueryCacheImpl(GridKernalContext ctx) {
        super(ctx);

        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
        Optional.ofNullable(ctx.internalSubscriptionProcessor()).ifPresent(this::registerListeners);
    }

    /** {@inheritDoc} */
    @Override public QueryPlan queryPlan(IgniteCalciteContext ctx, CacheKey key, Function<IgniteCalciteContext, QueryPlan> factory) {
        Map<CacheKey, QueryPlan> cache = this.cache;

        QueryPlan template = cache.get(key);

        if (template != null)
            return QueryPlan.fromTemplate(template, ctx);

        QueryPlan plan = factory.apply(ctx);

        cache.putIfAbsent(key, plan.template());

        return plan;
    }

    /**
     * Clear cached plans.
     */
    public void clear() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDrop(String schemaName) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onSchemaCreate(String schemaName) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        // No-op
    }

    /** */
    private void registerListeners(GridInternalSubscriptionProcessor prc) {
        prc.registerSchemaChangeListener(this);

    }
}
