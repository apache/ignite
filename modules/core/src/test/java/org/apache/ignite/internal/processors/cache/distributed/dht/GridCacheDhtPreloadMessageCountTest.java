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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadMessageCountTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 1000;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = CacheRebalanceMode.SYNC;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        assert preloadMode != null;

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(preloadMode);
        cc.setAffinity(new RendezvousAffinityFunction(false, 521));
        cc.setBackups(1);
        cc.setOnheapCacheEnabled(true);

        c.setCacheConfiguration(cc);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.record(GridDhtPartitionsSingleMessage.class);

        c.setCommunicationSpi(commSpi);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAutomaticPreload() throws Exception {
        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        IgniteCache<String, Integer> c0 = g0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        U.sleep(1000);

        IgniteCache<String, Integer> c1 = g1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<String, Integer> c2 = g2.cache(DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi spi0 = (TestRecordingCommunicationSpi)g0.configuration().getCommunicationSpi();
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi)g1.configuration().getCommunicationSpi();
        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi)g2.configuration().getCommunicationSpi();

        info(spi0.recordedMessages(false).size() + " " +
            spi1.recordedMessages(false).size() + " " +
            spi2.recordedMessages(false).size());

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * Checks if keys are present.
     *
     * @param c Cache.
     * @param keyCnt Key count.
     */
    private void checkCache(IgniteCache<String, Integer> c, int keyCnt) {
        Ignite g = c.unwrap(Ignite.class);

        for (int i = 0; i < keyCnt; i++) {
            String key = Integer.toString(i);

            if (affinity(c).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(Integer.valueOf(i), c.localPeek(key, CachePeekMode.ONHEAP));
        }
    }
}
