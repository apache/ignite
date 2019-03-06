/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Metrics test for partitioned cache with disabled near cache.
 */
public class GridCachePartitionedNearDisabledMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(gridCount() - 1);
        cfg.setRebalanceMode(SYNC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    // TODO: extend from GridCacheTransactionalAbstractMetricsSelfTest and uncomment:

//    /** {@inheritDoc} */
//    @Override protected int expectedReadsPerPut(boolean isPrimary) {
//        return 1;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int expectedMissesPerPut(boolean isPrimary) {
//        return 1;
//    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-819")
    @Test
    public void testGettingRemovedKey() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(0, 0);

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            // TODO: getting of removed key will produce 3 inner read operations.
            g.cache(DEFAULT_CACHE_NAME).removeAll();

            // TODO: getting of removed key will produce inner write and 4 inner read operations.
            //((IgniteKernal)g).cache(DEFAULT_CACHE_NAME).remove(0);

            assert g.cache(DEFAULT_CACHE_NAME).localSize() == 0;

            g.cache(DEFAULT_CACHE_NAME).mxBean().clear();
        }

        assertNull("Value is not null for key: " + 0, cache.get(0));

        // Check metrics for the whole cache.
        long removes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            removes += m.getCacheRemovals();
            reads += m.getCacheGets();
            hits += m.getCacheHits();
            misses += m.getCacheMisses();
        }

        assertEquals(0, removes);
        assertEquals(1, reads);
        assertEquals(0, hits);
        assertEquals(1, misses);
    }
}
