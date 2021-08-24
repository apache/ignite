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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.DO_NOTHING_CACHE_DATA_ROW_CONSUMER;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;

/**
 * Extension {@link IgniteH2Indexing} for the tests.
 */
public class IgniteH2IndexingEx extends IgniteH2Indexing {
    /**
     * Consumer for cache rows when creating an index on a node.
     * Mapping: Node name -> Index name -> Consumer.
     */
    private static final Map<String, Map<String, IgniteThrowableConsumer<CacheDataRow>>> idxCreateCacheRowConsumer =
        new ConcurrentHashMap<>();

    /**
     * Set {@link IgniteH2IndexingEx} to {@link GridQueryProcessor#idxCls} before starting the node.
     */
    static void prepareBeforeNodeStart() {
        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;
    }

    /**
     * Cleaning of internal structures. It is recommended to clean at
     * {@code GridCommonAbstractTest#beforeTest} and {@code GridCommonAbstractTest#afterTest}.
     *
     * @param nodeNamePrefix Prefix of node name ({@link GridKernalContext#igniteInstanceName()})
     *      for which internal structures will be removed, if {@code null} will be removed for all.
     *
     * @see GridCommonAbstractTest#getTestIgniteInstanceName()
     */
    static void clean(@Nullable String nodeNamePrefix) {
        if (nodeNamePrefix == null)
            idxCreateCacheRowConsumer.clear();
        else
            idxCreateCacheRowConsumer.entrySet().removeIf(e -> e.getKey().startsWith(nodeNamePrefix));
    }

    /**
     * Registering a consumer for cache rows when creating an index on a node.
     *
     * @param nodeName The name of the node,
     *      the value of which will return {@link GridKernalContext#igniteInstanceName()}.
     * @param idxName Index name.
     * @param c Cache row consumer.
     *
     * @see IndexingTestUtils#nodeName(GridKernalContext)
     * @see IndexingTestUtils#nodeName(IgniteEx)
     * @see IndexingTestUtils#nodeName(GridCacheContext)
     * @see GridCommonAbstractTest#getTestIgniteInstanceName(int)
     */
    static void addIdxCreateCacheRowConsumer(
        String nodeName,
        String idxName,
        IgniteThrowableConsumer<CacheDataRow> c
    ) {
        idxCreateCacheRowConsumer.computeIfAbsent(nodeName, s -> new ConcurrentHashMap<>()).put(idxName, c);
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(
        String schemaName,
        String tblName,
        QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor
    ) throws IgniteCheckedException {
        super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, clo -> {
            cacheVisitor.visit(row -> {
                idxCreateCacheRowConsumer
                    .getOrDefault(nodeName(ctx), emptyMap())
                    .getOrDefault(idxDesc.name(), DO_NOTHING_CACHE_DATA_ROW_CONSUMER)
                    .accept(row);

                clo.apply(row);
            });
        });
    }
}
