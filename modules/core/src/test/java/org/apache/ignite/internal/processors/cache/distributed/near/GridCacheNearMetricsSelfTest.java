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

package org.apache.ignite.internal.processors.cache.distributed.near;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Cache metrics test.
 */
public class GridCacheNearMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 50;

    /** {@inheritDoc} */
    @Override protected CacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /** */
    protected boolean perEntryMetricsEnabled() {
        return true;
    }

    /**
     * @return Key count.
     */
    protected int keyCount() {
        return KEY_CNT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(DEFAULT_CACHE_NAME).removeAll();

            assert g.cache(DEFAULT_CACHE_NAME).localSize() == 0;

            g.cache(DEFAULT_CACHE_NAME).localMxBean().clear();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        super.beforeTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            IgniteCache cache = g.cache(DEFAULT_CACHE_NAME);

            cache.enableStatistics(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cc = super.cacheConfiguration(igniteInstanceName);

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setBackups(1);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheDoesNotAffectCacheSize() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        IgniteEx g1 = grid(1);

        IgniteCache<Integer, Integer> cache1 = g1.cache(DEFAULT_CACHE_NAME);

        ClusterNode localNode = g1.cluster().localNode();

        int beforeSize = cache1.localMetrics().getSize();

        for (int i = 0; i < 100; i++) {
            if (!affinity(cache1).isPrimaryOrBackup(localNode, i))
                cache1.get(i); // put entry to near cache
        }

        assertEquals(beforeSize, cache1.localMetrics().getSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isPrimary(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read

                cache0.get(i); // +1 read.

                key = i;

                info("Puts: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            info("Puts: " + jcache.localMetrics().getCachePuts());
            info("Reads: " + jcache.localMetrics().getCacheGets());

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.localMetrics().getCachePuts());
            else
                assertEquals(0, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.localMetrics().getCacheGets());
                assertEquals(1, jcache.localMetrics().getCacheHits());
                assertEquals(1, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isBackup(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Puts: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);
            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.localMetrics().getCachePuts());
            else
                assertEquals(0, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(1, jcache.localMetrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getCacheGets());
                assertEquals(1, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            assertEquals(1, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(1, jcache.localMetrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(1, jcache.localMetrics().getCacheGets());
                assertEquals(1, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isPrimary(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            info("Writes: " + jcache.localMetrics().getCachePuts());
            info("Reads: " + jcache.localMetrics().getCacheGets());

            assertEquals(0, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(2, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            assertEquals(0, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(2, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.localMetrics().getCachePuts());
                info("Reads: " + cache0.localMetrics().getCacheGets());
                info("Hits: " + cache0.localMetrics().getCacheHits());
                info("Misses: " + cache0.localMetrics().getCacheMisses());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            assertEquals(0, jcache.localMetrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(2, jcache.localMetrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(0, jcache.localMetrics().getCacheMisses());
            }
            else {
                assertEquals(2, jcache.localMetrics().getCacheGets());
                assertEquals(0, jcache.localMetrics().getCacheHits());
                assertEquals(2, jcache.localMetrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateReadRemoveInvokesFromPrimary() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key = primaryKey(cache0);

        setValue1ByEntryProcessor(cache0, key);

        readKeyByEntryProcessor(cache0, key);

        removeKeyByEntryProcessor(cache0, key);

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
            else
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(3, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());
                assertEquals(2, jcache.localMetrics().getEntryProcessorHits());

                assertEquals((float) 1 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals((float) 2 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());
                assertEquals(1, jcache.localMetrics().getEntryProcessorHits());

                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            }
            else
                assertNoMetricsChanged(jcache);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateReadRemoveInvokesFromBackup() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key = backupKey(cache0);

        setValue1ByEntryProcessor(cache0, key);

        readKeyByEntryProcessor(cache0, key);

        removeKeyByEntryProcessor(cache0, key);

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
            else
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(3, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());
                assertEquals(2, jcache.localMetrics().getEntryProcessorHits());

                assertEquals((float) 1 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals((float) 2 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());
                assertEquals(1, jcache.localMetrics().getEntryProcessorHits());

                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            }
            else
                assertNoMetricsChanged(jcache);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateReadRemoveInvokesFromNear() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                setValue1ByEntryProcessor(cache0, i);

                readKeyByEntryProcessor(cache0, i);

                removeKeyByEntryProcessor(cache0, i);

                key = i;

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            assertEquals(1, jcache.localMetrics().getEntryProcessorPuts());
            assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
            assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(3, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(2, jcache.localMetrics().getEntryProcessorHits());

                assertEquals((float) 1 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals((float) 2 / 3 * 100.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            } else {
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorHits());

                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
                assertEquals(50.0f, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadRemoveInvokesFromPrimary() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key = primaryKey(cache0);

        readKeyByEntryProcessor(cache0, key);

        removeKeyByEntryProcessor(cache0, key);

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());

                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(2, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(1, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else
                assertNoMetricsChanged(jcache);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadRemoveInvokesFromBackup() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key = backupKey(cache0);

        readKeyByEntryProcessor(cache0, key);

        removeKeyByEntryProcessor(cache0, key);

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(2, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {

                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else
                assertNoMetricsChanged(jcache);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadRemoveInvokesFromNear() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(DEFAULT_CACHE_NAME);

        int key;

        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                readKeyByEntryProcessor(cache0, i);

                removeKeyByEntryProcessor(cache0, i);

                key = i;

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(DEFAULT_CACHE_NAME);

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(1, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(2, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(2, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)) {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
            else {
                assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
                assertEquals(1, jcache.localMetrics().getEntryProcessorRemovals());
                assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
                assertEquals(1, jcache.localMetrics().getEntryProcessorInvocations());

                assertEquals(1, jcache.localMetrics().getEntryProcessorMisses());

                assertEquals(100.0f, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
            }
        }
    }

    /**
     * Checks no metrics changed in cache.
     *
     * @param jcache Cache to be checked.
     */
    private void assertNoMetricsChanged(IgniteCache<Object, Object> jcache) {
        assertEquals(0, jcache.localMetrics().getEntryProcessorPuts());
        assertEquals(0, jcache.localMetrics().getEntryProcessorRemovals());
        assertEquals(0, jcache.localMetrics().getEntryProcessorReadOnlyInvocations());
        assertEquals(0, jcache.localMetrics().getEntryProcessorInvocations());

        assertEquals(0, jcache.localMetrics().getEntryProcessorMisses());
        assertEquals(0, jcache.localMetrics().getEntryProcessorHits());

        assertEquals(0, jcache.localMetrics().getEntryProcessorMissPercentage(), 0.001f);
        assertEquals(0, jcache.localMetrics().getEntryProcessorHitPercentage(), 0.001f);
    }

    /**
     * Invokes entry processor, which removes key from cache.
     *
     * @param cache Cache.
     * @param key Key.
     */
    private void removeKeyByEntryProcessor(IgniteCache<Integer, Integer> cache, int key) {
        cache.invoke(key, new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry,
                Object... arguments) throws EntryProcessorException {
                entry.remove();

                return null;
            }
        });
    }

    /**
     * Invokes entry processor, which reads key from cache.
     *
     * @param cache Cache.
     * @param key Key.
     */
    private void readKeyByEntryProcessor(IgniteCache<Integer, Integer> cache, int key) {
        cache.invoke(key, new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry,
                Object... arguments) throws EntryProcessorException {
                entry.getValue();

                return null;
            }
        });
    }

    /**
     * Invokes entry processor, which sets value "1" for key into cache.
     *
     * @param cache Cache.
     * @param key Key.
     */
    private void setValue1ByEntryProcessor(IgniteCache<Integer, Integer> cache, int key) {
        cache.invoke(key, new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry,
                Object... arguments) throws EntryProcessorException {
                entry.setValue(1);

                return null;
            }
        });
    }
}
