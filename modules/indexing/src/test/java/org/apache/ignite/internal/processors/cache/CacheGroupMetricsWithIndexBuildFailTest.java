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

package org.apache.ignite.internal.processors.cache;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Tests cache group metrics for index build fail case.
 */
public class CacheGroupMetricsWithIndexBuildFailTest extends AbstractIndexingCommonTest {
    /** Group name. */
    private static final String GROUP_NAME = "TEST_GROUP";

    /** {@code True} if fail index build. */
    private final AtomicBoolean failIndexRebuild = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024))
            )
            .setIndexingSpi(new TestIndexingSpi())
            .setBuildIndexThreadPoolSize(4);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setGroupName(GROUP_NAME);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
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

    /** */
    @Test
    public void testIndexRebuildCountPartitionsLeft() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        IgniteCache<Integer, Integer> cache1 = ignite0.getOrCreateCache(cacheConfiguration(cacheName1));
        IgniteCache<Integer, Integer> cache2 = ignite0.getOrCreateCache(cacheConfiguration(cacheName2));

        cache1.put(1, 1);
        cache2.put(1, 1);

        int parts1 = ignite0.cachex(cacheName1).configuration().getAffinity().partitions();
        int parts2 = ignite0.cachex(cacheName2).configuration().getAffinity().partitions();

        List<Path> idxPaths = getIndexBinPaths(cacheName1);

        stopAllGrids();

        idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        MetricRegistry grpMreg = ignite.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX, GROUP_NAME));

        LongMetric indexBuildCountPartitionsLeft = grpMreg.findMetric("IndexBuildCountPartitionsLeft");

        assertEquals(parts1 + parts2, indexBuildCountPartitionsLeft.value());

        failIndexRebuild.set(true);

        ((AbstractIndexingCommonTest.BlockingIndexing)ignite.context().query().getIndexing()).stopBlock(cacheName1);

        GridTestUtils.assertThrows(log, () -> ignite.cache(cacheName1).indexReadyFuture().get(30_000),
            IgniteSpiException.class, "Test exception.");

        assertEquals(parts2, indexBuildCountPartitionsLeft.value());

        failIndexRebuild.set(false);

        ((AbstractIndexingCommonTest.BlockingIndexing)ignite.context().query().getIndexing()).stopBlock(cacheName2);

        ignite.cache(cacheName2).indexReadyFuture().get(30_000);

        assertEquals(0, indexBuildCountPartitionsLeft.value());
    }

    /** */
    private class TestIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            if (failIndexRebuild.get())
                throw new IgniteSpiException("Test exception.");
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }
}
