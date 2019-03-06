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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Base class for eviction tests.
 */
public class GridCacheEvictionFilterSelfTest extends GridCommonAbstractTest {
    /** Replicated cache. */
    private CacheMode mode = REPLICATED;

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** */
    private EvictionFilter filter;

    /** Policy. */
    private EvictionPolicy<Object, Object> plc = new EvictionPolicy<Object, Object>() {
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry entry) {
            assert !(entry.getValue() instanceof Integer);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setEvictionPolicy(notSerializableProxy(plc, EvictionPolicy.class));
        cc.setOnheapCacheEnabled(true);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictionFilter(notSerializableProxy(filter, org.apache.ignite.cache.eviction.EvictionFilter.class));
        cc.setRebalanceMode(SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);

        if (nearEnabled) {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();
            nearCfg.setNearEvictionPolicy(notSerializableProxy(plc, EvictionPolicy.class));

            cc.setNearConfiguration(nearCfg);
        }
        else
            cc.setNearConfiguration(null);

        if (mode == PARTITIONED)
            cc.setBackups(1);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocal() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        mode = LOCAL;

        checkEvictionFilter();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicated() throws Exception {
        mode = REPLICATED;

        checkEvictionFilter();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitioned() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        mode = PARTITIONED;
        nearEnabled = true;

        checkEvictionFilter();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionedNearDisabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;

        checkEvictionFilter();
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("BusyWait")
    private void checkEvictionFilter() throws Exception {
        filter = new EvictionFilter();

        startGridsMultiThreaded(2);

        try {
            Ignite g = grid(0);

            IgniteCache<Object, Object> c = g.cache(DEFAULT_CACHE_NAME);

            int cnt = 1;

            for (int i = 0; i < cnt; i++)
                c.put(i, i);

            Map<Object, AtomicInteger> cnts = filter.counts();

            int exp = mode == LOCAL ? 1 : mode == REPLICATED ? 2 : nearEnabled ? 3 : 2;

            for (int j = 0; j < 3; j++) {
                boolean success = true;

                for (int i = 0; i < cnt; i++) {
                    int cnt0 = cnts.get(i).get();

                    success = cnt0 == exp;

                    if (!success) {
                        U.warn(log, "Invalid count for key [key=" + i + ", cnt=" + cnt0 + ", expected=" + exp + ']');

                        break;
                    }
                    else
                        info("Correct count for key [key=" + i + ", cnt=" + cnt0 + ']');
                }

                if (success)
                    break;

                if (j < 2)
                    Thread.sleep(1000);
                else
                    assert false : "Test has not succeeded (see log for details).";
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * This test case is just to visualize a support issue from client. It does not fail.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedMixed() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;

        filter = new EvictionFilter();

        Ignite g = startGrid();

        IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

        try {
            int id = 1;

            cache.put(id++, 1);
            cache.put(id++, 2);

            for (int i = id + 1; i < 10; i++) {
                cache.put(id, id);

                cache.put(i, String.valueOf(i));
            }

            info(">>>> " + cache.get(1));
            info(">>>> " + cache.get(2));
            info(">>>> " + cache.get(3));
        }
        finally {
            stopGrid();
        }
    }

    /**
     *
     */
    private final class EvictionFilter implements org.apache.ignite.cache.eviction.EvictionFilter<Object, Object> {
        /** */
        private final ConcurrentMap<Object, AtomicInteger> cnts = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry<Object, Object> entry) {
            AtomicInteger i = cnts.get(entry.getKey());

            if (i == null) {
                AtomicInteger old = cnts.putIfAbsent(entry.getKey(), i = new AtomicInteger());

                if (old != null)
                    i = old;
            }

            i.incrementAndGet();

            boolean ret = !(entry.getValue() instanceof Integer);

            if (!ret)
                info(">>> Not evicting key [key=" + entry.getKey() + ", cnt=" + i.get() + ']');
            else
                info(">>> Evicting key [key=" + entry.getKey() + ", cnt=" + i.get() + ']');

            return ret;
        }

        /** @return Counts. */
        ConcurrentMap<Object, AtomicInteger> counts() {
            return cnts;
        }
    }
}
