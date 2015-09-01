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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /** {@inheritDoc} */
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

            g.cache(null).removeAll();

            assert g.cache(null).localSize() == 0;

            g.cache(null).mxBean().clear();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(null).getConfiguration(CacheConfiguration.class).setStatisticsEnabled(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setBackups(1);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isPrimary(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read

                cache0.get(i); // +1 read.

                key = i;

                info("Puts: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            IgniteCache<Object, Object> jcache = g.cache(null);

            info("Puts: " + jcache.metrics().getCachePuts());
            info("Reads: " + jcache.metrics().getCacheGets());

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.metrics().getCachePuts());
            else
                assertEquals(0, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.metrics().getCacheGets());
                assertEquals(1, jcache.metrics().getCacheHits());
                assertEquals(1, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isBackup(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Puts: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);
            IgniteCache<Object, Object> jcache = g.cache(null);

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, jcache.metrics().getCachePuts());
            else
                assertEquals(0, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(1, jcache.metrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)){
                assertEquals(1, jcache.metrics().getCacheGets());
                assertEquals(1, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearPut() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.getAndPut(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(null);

            assertEquals(1, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(1, jcache.metrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)){
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(1, jcache.metrics().getCacheGets());
                assertEquals(1, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isPrimary(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            IgniteCache<Object, Object> jcache = g.cache(null);

            info("Writes: " + jcache.metrics().getCachePuts());
            info("Reads: " + jcache.metrics().getCacheGets());

            assertEquals(0, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(2, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (affinity(cache0).isBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(null);

            assertEquals(0, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimaryOrBackup(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(2, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRead() throws Exception {
        Ignite g0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!affinity(cache0).isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().getCachePuts());
                info("Reads: " + cache0.metrics().getCacheGets());
                info("Hits: " + cache0.metrics().getCacheHits());
                info("Misses: " + cache0.metrics().getCacheMisses());
                info("Affinity nodes: " + U.nodes2names(affinity(cache0).mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            IgniteCache<Object, Object> jcache = g.cache(null);

            assertEquals(0, jcache.metrics().getCachePuts());

            if (affinity(jcache).isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(2, jcache.metrics().getCacheMisses());
            }
            else if (affinity(jcache).isBackup(g.cluster().localNode(), key)){
                assertEquals(0, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(0, jcache.metrics().getCacheMisses());
            }
            else {
                assertEquals(2, jcache.metrics().getCacheGets());
                assertEquals(0, jcache.metrics().getCacheHits());
                assertEquals(2, jcache.metrics().getCacheMisses());
            }
        }
    }
}