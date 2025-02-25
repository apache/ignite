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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.BreakBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.SlowdownBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.SortedIndexDescriptorFactory;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IndexesRebuildTaskEx.addCacheRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.DO_NOTHING_CACHE_DATA_ROW_CONSUMER;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Base class for testing index rebuilds.
 */
public abstract class AbstractRebuildIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        SchemaManager.registerIndexDescriptorFactory(QueryIndexType.SORTED, new SortedIndexDescriptorFactoryEx(log));
        SortedIndexDescriptorFactoryEx.clean(getTestIgniteInstanceName());
        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        SchemaManager.unregisterIndexDescriptorFactory(QueryIndexType.SORTED);
        SortedIndexDescriptorFactoryEx.clean(getTestIgniteInstanceName());
        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(cacheCfg(DEFAULT_CACHE_NAME, null));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx n = super.startGrid(idx);

        n.cluster().state(ACTIVE);

        return n;
    }

    /**
     * Registering a {@link StopBuildIndexConsumer} to {@link IndexesRebuildTaskEx#addCacheRowConsumer}.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @return New instance of {@link StopBuildIndexConsumer}.
     */
    protected StopBuildIndexConsumer addStopRebuildIndexConsumer(IgniteEx n, String cacheName) {
        StopBuildIndexConsumer consumer = new StopBuildIndexConsumer(getTestTimeout());

        addCacheRowConsumer(nodeName(n), cacheName, consumer);

        return consumer;
    }

    /**
     * Registering a {@link BreakBuildIndexConsumer} to {@link IndexesRebuildTaskEx#addCacheRowConsumer}.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @param breakCnt Count of rows processed, after which an {@link IgniteCheckedException} will be thrown.
     * @return New instance of {@link BreakBuildIndexConsumer}.
     */
    protected BreakBuildIndexConsumer addBreakRebuildIndexConsumer(IgniteEx n, String cacheName, int breakCnt) {
        BreakBuildIndexConsumer consumer = new BreakBuildIndexConsumer(
            getTestTimeout(),
            (c, r) -> c.visitCnt.get() >= breakCnt
        );

        addCacheRowConsumer(nodeName(n), cacheName, consumer);

        return consumer;
    }

    /**
     * Registering a {@link SlowdownBuildIndexConsumer} to {@link IndexesRebuildTaskEx#addCacheRowConsumer}.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @param sleepTime Sleep time after processing each cache row in milliseconds.
     * @return New instance of {@link SlowdownBuildIndexConsumer}.
     */
    protected SlowdownBuildIndexConsumer addSlowdownRebuildIndexConsumer(
        IgniteEx n,
        String cacheName,
        long sleepTime
    ) {
        SlowdownBuildIndexConsumer consumer = new SlowdownBuildIndexConsumer(getTestTimeout(), sleepTime);

        addCacheRowConsumer(nodeName(n), cacheName, consumer);

        return consumer;
    }

    /**
     * Registering a {@link SlowdownBuildIndexConsumer} to
     * {@link SortedIndexDescriptorFactoryEx#addIdxCreateCacheRowConsumer}.
     *
     * @param n Node.
     * @param idxName Index name.
     * @param sleepTime Sleep time after processing each cache row in milliseconds.
     * @return New instance of {@link SlowdownBuildIndexConsumer}.
     */
    protected SlowdownBuildIndexConsumer addSlowdownIdxCreateConsumer(IgniteEx n, String idxName, long sleepTime) {
        SlowdownBuildIndexConsumer consumer = new SlowdownBuildIndexConsumer(getTestTimeout(), sleepTime);

        SortedIndexDescriptorFactoryEx.addIdxCreateCacheRowConsumer(nodeName(n), idxName, consumer);

        return consumer;
    }

    /**
     * Registering a {@link BreakBuildIndexConsumer} to
     * {@link SortedIndexDescriptorFactoryEx#addIdxCreateCacheRowConsumer}.
     *
     * @param n Node.
     * @param idxName Index name.
     * @param breakCnt Count of rows processed, after which an {@link IgniteCheckedException} will be thrown.
     * @return New instance of {@link BreakBuildIndexConsumer}.
     */
    protected BreakBuildIndexConsumer addBreakIdxCreateConsumer(IgniteEx n, String idxName, int breakCnt) {
        BreakBuildIndexConsumer consumer = new BreakBuildIndexConsumer(
            getTestTimeout(),
            (c, r) -> c.visitCnt.get() >= breakCnt
        );

        SortedIndexDescriptorFactoryEx.addIdxCreateCacheRowConsumer(nodeName(n), idxName, consumer);

        return consumer;
    }

    /**
     * Checking that rebuilding indexes for the cache has started.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @return Rebuild index future.
     */
    protected IgniteInternalFuture<?> checkStartRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx) {
        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, cacheCtx.cacheId());

        assertNotNull(idxRebFut);
        assertFalse(idxRebFut.isDone());

        checkCacheMetrics0(n, cacheCtx.name(), true, 0L);

        return idxRebFut;
    }

    /**
     * Checking metrics rebuilding indexes of cache.
     *
     * @param n                          Node.
     * @param cacheName                  Cache name.
     * @param expIdxRebuildInProgress    The expected status of rebuilding indexes.
     * @param expIdxRebuildKeysProcessed The expected number of keys processed during index rebuilding.
     */
    protected void checkCacheMetrics0(
        IgniteEx n,
        String cacheName,
        boolean expIdxRebuildInProgress,
        @Nullable Long expIdxRebuildKeysProcessed
    ) {
        CacheMetricsImpl metrics0 = cacheMetrics0(n, cacheName);
        assertNotNull(metrics0);

        assertEquals(expIdxRebuildInProgress, metrics0.isIndexRebuildInProgress());

        if (expIdxRebuildKeysProcessed != null)
            assertEquals(expIdxRebuildKeysProcessed.longValue(), metrics0.getIndexRebuildKeysProcessed());
    }

    /**
     * Checking that the rebuild of indexes for the cache has completed.
     *
     * @param n Node.
     * @param cacheCtx Cache context.
     * @param expKeys The expected number of keys processed during index rebuilding
     */
    protected void checkFinishRebuildIndexes(IgniteEx n, GridCacheContext<?, ?> cacheCtx, int expKeys) {
        assertNull(indexRebuildFuture(n, cacheCtx.cacheId()));

        checkCacheMetrics0(n, cacheCtx.name(), false, (long)expKeys);
    }

    /**
     * Stopping all nodes and deleting their index.bin.
     */
    protected void stopAllGridsWithDeleteIndexBin() {
        List<NodeFileTree> fts = G.allGrids().stream()
            .map(srv -> ((IgniteEx)srv).context().pdsFolderResolver().fileTree())
            .collect(toList());

        stopAllGrids();

        for (NodeFileTree ft : fts)
            deleteIndexBin(ft);
    }

    /**
     * Create cache configuration with index: {@link Integer} -> {@link Person}.
     *
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @return New instance of the cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheCfg(String cacheName, @Nullable String grpName) {
        CacheConfiguration<K, V> cacheCfg = new CacheConfiguration<>(cacheName);

        return cacheCfg.setGroupName(grpName).setIndexedTypes(Integer.class, Person.class);
    }

    /**
     * Populate cache with {@link Person} sequentially.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     */
    protected void populate(IgniteCache<Integer, Person> cache, int cnt) {
        for (int i = 0; i < cnt; i++)
            cache.put(i, new Person(i, "name_" + i));
    }

    /**
     * Getting {@code GridQueryProcessor#idxBuildStatusStorage}.
     *
     * @param n Node.
     * @return Index build status storage.
     */
    protected IndexBuildStatusStorage indexBuildStatusStorage(IgniteEx n) {
        return getFieldValue(n.context().query(), "idxBuildStatusStorage");
    }

    /**
     * Getting {@code IndexBuildStatusStorage#statuses}.
     *
     * @param n Node.
     * @return Index build status storage.
     */
    protected ConcurrentMap<String, IndexBuildStatusHolder> statuses(IgniteEx n) {
        return getFieldValue(indexBuildStatusStorage(n), "statuses");
    }

    /**
     * Creation of a new index for the cache of {@link Person}.
     * SQL: CREATE INDEX " + idxName + " ON Person(name)
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation result.
     */
    protected List<List<?>> createIdx(IgniteCache<Integer, Person> cache, String idxName) {
        String sql = "CREATE INDEX " + idxName + " ON Person(name)";

        return cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Enable checkpoints.
     *
     * @param n Node.
     * @param reason Reason for checkpoint wakeup if it would be required.
     * @param enable Enable/disable.
     */
    protected Void enableCheckpoints(IgniteEx n, String reason, boolean enable) throws Exception {
        if (enable) {
            dbMgr(n).enableCheckpoints(true).get(getTestTimeout());

            forceCheckpoint(F.asList(n), reason);
        }
        else {
            forceCheckpoint(F.asList(n), reason);

            dbMgr(n).enableCheckpoints(false).get(getTestTimeout());
        }

        return null;
    }

    /**
     * Getting index description.
     *
     * @param idx Index.
     * @return Index description.
     */
    protected SortedIndexDefinition indexDefinition(Index idx) {
        return getFieldValue(idx, "def");
    }

    /**
     * Getting the cache index.
     *
     * @param n Node.
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index.
     */
    @Nullable protected Index index(IgniteEx n, IgniteCache<Integer, Person> cache, String idxName) {
        return n.context().indexProcessor().indexes(cache.getName()).stream()
            .filter(i -> idxName.equals(i.name()))
            .findAny()
            .orElse(null);
    }

    /** */
    private static class SortedIndexDescriptorFactoryEx extends SortedIndexDescriptorFactory {
        /**
         * Consumer for cache rows when creating an index on a node.
         * Mapping: Node name -> Index name -> Consumer.
         */
        private static final Map<String, Map<String, IgniteThrowableConsumer<CacheDataRow>>> idxCreateCacheRowConsumer =
            new ConcurrentHashMap<>();

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

        /** */
        public SortedIndexDescriptorFactoryEx(IgniteLogger log) {
            super(log);
        }

        /** {@inheritDoc} */
        @Override public IndexDescriptor create(
            GridKernalContext ctx,
            GridQueryIndexDescriptor idxDesc,
            TableDescriptor tbl,
            @Nullable SchemaIndexCacheVisitor cacheVisitor
        ) {
            return super.create(ctx, idxDesc, tbl, clo -> {
                if (cacheVisitor != null) {
                    cacheVisitor.visit(row -> {
                        idxCreateCacheRowConsumer
                            .getOrDefault(nodeName(ctx), emptyMap())
                            .getOrDefault(idxDesc.name(), DO_NOTHING_CACHE_DATA_ROW_CONSUMER)
                            .accept(row);

                        clo.apply(row);
                    });
                }
            });
        }
    }
}
