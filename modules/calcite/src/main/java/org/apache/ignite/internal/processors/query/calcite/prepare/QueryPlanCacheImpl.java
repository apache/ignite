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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 *
 */
public class QueryPlanCacheImpl extends AbstractService implements QueryPlanCache, SchemaChangeListener {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** */
    private GridInternalSubscriptionProcessor subscriptionProcessor;

    /** */
    private volatile Map<CacheKey, QueryPlan> cache;

    /**
     * @param ctx Kernal context.
     */
    public QueryPlanCacheImpl(GridKernalContext ctx) {
        super(ctx);

        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
        subscriptionProcessor(ctx.internalSubscriptionProcessor());

        init();
    }

    /**
     * @param subscriptionProcessor Subscription processor.
     */
    public void subscriptionProcessor(GridInternalSubscriptionProcessor subscriptionProcessor) {
        this.subscriptionProcessor = subscriptionProcessor;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        subscriptionProcessor.registerSchemaChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public QueryPlan queryPlan(CacheKey key, Supplier<QueryPlan> planSupplier) {
        Map<CacheKey, QueryPlan> cache = this.cache;

        QueryPlan plan = cache.computeIfAbsent(key, k -> planSupplier.get());

        return plan.copy();
    }

    /** {@inheritDoc} */
    @Override public QueryPlan queryPlan(CacheKey key) {
        Map<CacheKey, QueryPlan> cache = this.cache;
        QueryPlan plan = cache.get(key);
        return plan != null ? plan.copy() : null;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDropped(String schemaName) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDescriptor,
        boolean destroy
    ) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onIndexCreated(String schemaName, String tblName, String idxName,
        GridQueryIndexDescriptor idxDesc, Index idx) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onSchemaCreated(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onColumnsAdded(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<QueryField> cols
    ) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onColumnsDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<String> cols
    ) {
        clear();
    }

    /** {@inheritDoc} */
    @Override public void onFunctionCreated(String schemaName, String name, Method method) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
        // No-op.
    }
}
