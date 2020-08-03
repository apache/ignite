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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.junit.Test;

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

        GridQueryProcessor.idxCls = null;
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

    /** @throws Exception If failed. */
    @Test
    public void testIndexRebuildingMetric() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        IgniteCache<KeyClass, ValueClass> cache1 = ignite.getOrCreateCache(cacheConfiguration(cacheName1));
        IgniteCache<KeyClass, ValueClass> cache2 = ignite.getOrCreateCache(cacheConfiguration(cacheName2));

        cache1.put(new KeyClass(1), new ValueClass(1L));
        cache2.put(new KeyClass(1), new ValueClass(1L));

        List<Path> idxPaths = getIndexBinPaths(cacheName1);

        idxPaths.addAll(getIndexBinPaths(cacheName2));

        stopAllGrids();

        idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        ignite = startGrid(0);

        BooleanMetric indexRebuildCache1 = isIndexRebuildInProgressMetric(ignite, cacheName1);
        BooleanMetric indexRebuildCache2 = isIndexRebuildInProgressMetric(ignite, cacheName2);

        ignite.cluster().active(true);

        assertTrue(indexRebuildCache1.value());
        assertTrue(indexRebuildCache2.value());

        ((BlockingIndexing)ignite.context().query().getIndexing()).stopBlock(cacheName1);

        ignite.cache(cacheName1).indexReadyFuture().get(30_000);

        assertFalse(indexRebuildCache1.value());
        assertTrue(indexRebuildCache2.value());

        ((BlockingIndexing)ignite.context().query().getIndexing()).stopBlock(cacheName2);

        ignite.cache(cacheName2).indexReadyFuture().get(30_000);

        assertFalse(indexRebuildCache1.value());
        assertFalse(indexRebuildCache2.value());
    }

    /** @return Gets {@code IsIndexRebuildInProgress} metric for given cache. */
    private BooleanMetric isIndexRebuildInProgressMetric(IgniteEx ignite, String cacheName) {
        MetricRegistry mreg = ignite.context().metric().registry(cacheMetricsRegistryName(cacheName, false));

        return mreg.findMetric("IsIndexRebuildInProgress");
    }
}
