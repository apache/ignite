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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.KeyClass;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.ValueClass;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Tests index metrics.
 */
public class IndexMetricsTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
            )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        IndexProcessor.idxRebuildCls = null;
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration<KeyClass, ValueClass> cacheConfiguration(String cacheName) {
        CacheConfiguration<KeyClass, ValueClass> ccfg = new CacheConfiguration<>(cacheName);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(KeyClass.class.getName());
        entity.setValueType(ValueClass.class.getName());

        entity.setKeyFieldName("key");

        entity.addQueryField("key", entity.getKeyType(), null);

        entity.setIndexes(Collections.singletonList(
            new QueryIndex("key", true, cacheName + "_index")
        ));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        return ccfg;
    }

    /**
     *
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexRebuildingMetric() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().state(ClusterState.ACTIVE);

        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        IgniteCache<KeyClass, ValueClass> cache1 = ig0.getOrCreateCache(cacheConfiguration(cacheName1));
        IgniteCache<KeyClass, ValueClass> cache2 = ig0.getOrCreateCache(cacheConfiguration(cacheName2));

        int entryCnt1 = 100;
        int entryCnt2 = 200;

        for (int i = 0; i < entryCnt1; i++)
            cache1.put(new KeyClass(i), new ValueClass((long)i));

        for (int i = 0; i < entryCnt2; i++)
            cache2.put(new KeyClass(i), new ValueClass((long)i));

        List<Path> idxPaths = getIndexBinPaths(cacheName1);

        idxPaths.addAll(getIndexBinPaths(cacheName2));

        stopAllGrids();

        idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;

        IgniteEx n = startGrid(0);

        BooleanMetric idxRebuildInProgress1 = indexRebuildMetric(n, cacheName1, "IsIndexRebuildInProgress");
        BooleanMetric idxRebuildInProgress2 = indexRebuildMetric(n, cacheName2, "IsIndexRebuildInProgress");

        LongAdderMetric idxRebuildKeyProcessed1 = indexRebuildMetric(n, cacheName1, "IndexRebuildKeyProcessed");
        LongAdderMetric idxRebuildKeyProcessed2 = indexRebuildMetric(n, cacheName2, "IndexRebuildKeyProcessed");

        CacheMetrics cacheMetrics1 = cacheMetrics(n, cacheName1);
        CacheMetrics cacheMetrics2 = cacheMetrics(n, cacheName2);

        n.cluster().state(ClusterState.ACTIVE);

        BooleanSupplier[] idxRebuildProgressCache1 = {
            idxRebuildInProgress1::value,
            cacheMetrics1::isIndexRebuildInProgress,
            () -> n.cache(cacheName1).localMetrics().isIndexRebuildInProgress()
        };

        BooleanSupplier[] idxRebuildProgressCache2 = {
            idxRebuildInProgress2::value,
            cacheMetrics2::isIndexRebuildInProgress,
            () -> n.cache(cacheName2).localMetrics().isIndexRebuildInProgress()
        };

        // It must always be false, because metric is only per node.
        BooleanSupplier[] idxRebuildProgressCluster = {
            () -> n.cache(cacheName1).metrics().isIndexRebuildInProgress(),
            () -> n.cache(cacheName2).metrics().isIndexRebuildInProgress()
        };

        LongSupplier[] idxRebuildKeyProcessedCache1 = {
            idxRebuildKeyProcessed1::value,
            cacheMetrics1::getIndexRebuildKeysProcessed,
            () -> n.cache(cacheName1).localMetrics().getIndexRebuildKeysProcessed()
        };

        LongSupplier[] idxRebuildKeyProcessedCache2 = {
            idxRebuildKeyProcessed2::value,
            cacheMetrics2::getIndexRebuildKeysProcessed,
            () -> n.cache(cacheName2).localMetrics().getIndexRebuildKeysProcessed()
        };

        // It must always be 0, because metric is only per node.
        LongSupplier[] idxRebuildKeyProcessedCluster = {
            () -> n.cache(cacheName1).metrics().getIndexRebuildKeysProcessed(),
            () -> n.cache(cacheName2).metrics().getIndexRebuildKeysProcessed()
        };

        assertEquals(true, idxRebuildProgressCache1);
        assertEquals(true, idxRebuildProgressCache2);
        assertEquals(false, idxRebuildProgressCluster);

        assertEquals(0, idxRebuildKeyProcessedCache1);
        assertEquals(0, idxRebuildKeyProcessedCache2);
        assertEquals(0, idxRebuildKeyProcessedCluster);

        ((BlockingIndexesRebuildTask)n.context().indexProcessor().idxRebuild()).stopBlock(cacheName1);

        n.cache(cacheName1).indexReadyFuture().get(30_000);

        assertEquals(false, idxRebuildProgressCache1);
        assertEquals(true, idxRebuildProgressCache2);
        assertEquals(false, idxRebuildProgressCluster);

        assertEquals(entryCnt1, idxRebuildKeyProcessedCache1);
        assertEquals(0, idxRebuildKeyProcessedCache2);
        assertEquals(0, idxRebuildKeyProcessedCluster);

        ((BlockingIndexesRebuildTask)n.context().indexProcessor().idxRebuild()).stopBlock(cacheName2);

        n.cache(cacheName2).indexReadyFuture().get(30_000);

        assertEquals(false, idxRebuildProgressCache1);
        assertEquals(false, idxRebuildProgressCache2);
        assertEquals(false, idxRebuildProgressCluster);

        assertEquals(entryCnt1, idxRebuildKeyProcessedCache1);
        assertEquals(entryCnt2, idxRebuildKeyProcessedCache2);
        assertEquals(0, idxRebuildKeyProcessedCluster);
    }

    /**
     * Get index rebuild metric.
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @param name Name of the metric.
     * @return Gets {@code IsIndexRebuildInProgress} metric for given cache.
     */
    private <M extends Metric> M indexRebuildMetric(IgniteEx ignite, String cacheName, String name) {
        MetricRegistry mreg = ignite.context().metric().registry(cacheMetricsRegistryName(cacheName, false));

        return mreg.findMetric(name);
    }

    /**
     * Get cache metrics.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Cache metrics.
     */
    private CacheMetrics cacheMetrics(IgniteEx node, String cacheName) {
        requireNonNull(node);
        requireNonNull(cacheName);

        return node.context().cache().cacheGroup(CU.cacheId(cacheName)).singleCacheContext().cache().metrics0();
    }

    /**
     * Assertion that expected value is equal with all actual values.
     *
     * @param exp Expected value.
     * @param actuals Suppliers of actual values.
     */
    private void assertEquals(boolean exp, BooleanSupplier... actuals) {
        requireNonNull(actuals);

        for (int i = 0; i < actuals.length; i++)
            assertEquals("i=" + i, exp, actuals[i].getAsBoolean());
    }

    /**
     * Assertion that expected value is equal with all actual values.
     *
     * @param exp Expected value.
     * @param actuals Suppliers of actual values.
     */
    private void assertEquals(long exp, LongSupplier... actuals) {
        requireNonNull(actuals);

        for (int i = 0; i < actuals.length; i++)
            assertEquals("i=" + i, exp, actuals[i].getAsLong());
    }
}
