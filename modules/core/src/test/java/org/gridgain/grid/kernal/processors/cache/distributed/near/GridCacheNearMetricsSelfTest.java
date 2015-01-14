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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache metrics test.
 */
public class GridCacheNearMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 50;

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
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

            assert g.cache(null).isEmpty();

            g.cache(null).resetMetrics();
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setBackups(1);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryPut() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (cache0.affinity().isPrimary(g0.cluster().localNode(), i)) {
                cache0.put(i, i); // +1 read

                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            info("Writes: " + g.cache(null).metrics().writes());
            info("Reads: " + g.cache(null).metrics().reads());

            if (g.cache(null).affinity().isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, g.cache(null).metrics().writes());
            else
                assertEquals(0, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(1, g.cache(null).metrics().hits());
                assertEquals(1, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupPut() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (cache0.affinity().isBackup(g0.cluster().localNode(), i)) {
                cache0.put(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            if (g.cache(null).affinity().isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(1, g.cache(null).metrics().writes());
            else
                assertEquals(0, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(1, g.cache(null).metrics().misses());
            }
            else if (g.cache(null).affinity().isBackup(g.cluster().localNode(), key)){
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(1, g.cache(null).metrics().hits());
                assertEquals(1, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearPut() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!cache0.affinity().isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.put(i, i); // +1 read.

                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            assertEquals(1, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimary(g.cluster().localNode(), key)) {
                assertEquals(1, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(1, g.cache(null).metrics().misses());
            }
            else if (g.cache(null).affinity().isBackup(g.cluster().localNode(), key)){
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(1, g.cache(null).metrics().hits());
                assertEquals(1, g.cache(null).metrics().misses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryRead() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (cache0.affinity().isPrimary(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            info("Checking grid: " + g.name());

            info("Writes: " + g.cache(null).metrics().writes());
            info("Reads: " + g.cache(null).metrics().reads());

            assertEquals(0, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(2, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupRead() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (cache0.affinity().isBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                cache0.get(i); // +1 read.

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            assertEquals(0, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimaryOrBackup(g.cluster().localNode(), key)) {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(2, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearRead() throws Exception {
        Ignite g0 = grid(0);

        GridCache<Integer, Integer> cache0 = g0.cache(null);

        int key;

        // Put and get a few keys.
        for (int i = 0; ; i++) {
            if (!cache0.affinity().isPrimaryOrBackup(g0.cluster().localNode(), i)) {
                cache0.get(i); // +1 read.
                cache0.get(i); // +1 read.

                key = i;

                info("Writes: " + cache0.metrics().writes());
                info("Reads: " + cache0.metrics().reads());
                info("Hits: " + cache0.metrics().hits());
                info("Misses: " + cache0.metrics().misses());
                info("Affinity nodes: " + U.nodes2names(cache0.affinity().mapKeyToPrimaryAndBackups(i)));

                break;
            }
        }

        for (int j = 0; j < gridCount(); j++) {
            Ignite g = grid(j);

            assertEquals(0, g.cache(null).metrics().writes());

            if (g.cache(null).affinity().isPrimary(g.cluster().localNode(), key)) {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(2, g.cache(null).metrics().misses());
            }
            else if (g.cache(null).affinity().isBackup(g.cluster().localNode(), key)){
                assertEquals(0, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(0, g.cache(null).metrics().misses());
            }
            else {
                assertEquals(2, g.cache(null).metrics().reads());
                assertEquals(0, g.cache(null).metrics().hits());
                assertEquals(2, g.cache(null).metrics().misses());
            }
        }
    }
}
