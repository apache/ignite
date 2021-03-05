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
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheCompoundFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheStat;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for checking the correct completion/stop of index rebuilding.
 */
public class StopRebuildIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteH2IndexingEx.cacheRowConsumer.clear();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteH2IndexingEx.cacheRowConsumer.clear();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
            );
    }

    /**
     * Checks the correctness {@link SchemaIndexCacheCompoundFuture}.
     */
    @Test
    public void testSchemaIndexCacheCompoundFeature() {
        SchemaIndexCacheCompoundFuture compoundFut = new SchemaIndexCacheCompoundFuture();
        assertFalse(compoundFut.isDone());

        GridFutureAdapter<SchemaIndexCacheStat> fut0 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut1 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut2 = new GridFutureAdapter<>();
        GridFutureAdapter<SchemaIndexCacheStat> fut3 = new GridFutureAdapter<>();

        compoundFut.add(fut0).add(fut1).add(fut2).add(fut3);
        assertFalse(compoundFut.isDone());

        fut0.onDone();
        assertFalse(compoundFut.isDone());

        fut1.onDone();
        assertFalse(compoundFut.isDone());

        fut2.onDone();
        assertFalse(compoundFut.isDone());

        fut3.onDone();
        assertFalse(compoundFut.isDone());

        compoundFut.markInitialized();
        assertTrue(compoundFut.isDone());
        assertNull(compoundFut.error());

        compoundFut = new SchemaIndexCacheCompoundFuture();
        fut0 = new GridFutureAdapter<>();
        fut1 = new GridFutureAdapter<>();
        fut2 = new GridFutureAdapter<>();
        fut3 = new GridFutureAdapter<>();

        compoundFut.add(fut0).add(fut1).add(fut2).add(fut3).markInitialized();
        assertFalse(compoundFut.isDone());

        fut0.onDone();
        assertFalse(compoundFut.isDone());

        Exception err0 = new Exception();
        Exception err1 = new Exception();

        fut1.onDone(err0);
        assertFalse(compoundFut.isDone());

        fut2.onDone(err1);
        assertFalse(compoundFut.isDone());

        fut3.onDone(err1);
        assertTrue(compoundFut.isDone());
        assertEquals(err0, compoundFut.error().getCause());
    }

    /**
     * Checking that when the cluster is deactivated, index rebuilding will be completed correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopRebuildIndexesOnDeactivation() throws Exception {
        stopRebuildIndexes(n -> n.cluster().state(INACTIVE));

        assertEquals(1, G.allGrids().size());
    }

    /**
     * Checking that when the node stopped, index rebuilding will be completed correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopRebuildIndexesOnStopNode() throws Exception {
        stopRebuildIndexes(n -> stopAllGrids());
    }

    /**
     * Restart the rebuild of the indexes, checking that it completes gracefully.
     *
     * @param stopRebuildIndexes Stop index rebuild function.
     * @throws Exception If failed.
     */
    private void stopRebuildIndexes(IgniteThrowableConsumer<IgniteEx> stopRebuildIndexes) throws Exception {
        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;

        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        int keys = 100_000;

        for (int i = 0; i < keys; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "p_" + i));

        IgniteH2IndexingEx.cacheRowConsumer.put(DEFAULT_CACHE_NAME, row -> {
            U.sleep(10);
        });

        n.context().cache().context().database().forceRebuildIndexes(F.asList(n.cachex(DEFAULT_CACHE_NAME).context()));

        IgniteInternalFuture<?> idxFut = n.context().query().indexRebuildFuture(CU.cacheId(DEFAULT_CACHE_NAME));
        assertNotNull(idxFut);

        CacheMetricsImpl metrics0 = n.cachex(DEFAULT_CACHE_NAME).context().cache().metrics0();
        assertTrue(metrics0.isIndexRebuildInProgress());
        assertFalse(idxFut.isDone());

        assertTrue(waitForCondition(() -> metrics0.getIndexRebuildKeysProcessed() >= keys / 100, getTestTimeout()));
        assertTrue(metrics0.isIndexRebuildInProgress());
        assertFalse(idxFut.isDone());

        stopRebuildIndexes.accept(n);

        assertFalse(metrics0.isIndexRebuildInProgress());
        assertTrue(metrics0.getIndexRebuildKeysProcessed() < keys);

        // It is expected that there will be no errors.
        idxFut.get(getTestTimeout());
    }

    /**
     * Extension {@link IgniteH2Indexing} for the test.
     */
    private static class IgniteH2IndexingEx extends IgniteH2Indexing {
        /** Consumer for cache lines when rebuilding indexes. */
        private static Map<String, IgniteThrowableConsumer<CacheDataRow>> cacheRowConsumer = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(
            GridCacheContext cctx,
            SchemaIndexCacheVisitorClosure clo,
            GridFutureAdapter<Void> rebuildIdxFut
        ) {
            super.rebuildIndexesFromHash0(cctx, new SchemaIndexCacheVisitorClosure() {
                /** {@inheritDoc} */
                @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                    cacheRowConsumer.getOrDefault(cctx.name(), r -> {}).accept(row);

                    clo.apply(row);
                }
            }, rebuildIdxFut);
        }
    }
}
