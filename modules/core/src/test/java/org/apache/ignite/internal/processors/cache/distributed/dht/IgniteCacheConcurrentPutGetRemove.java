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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheConcurrentPutGetRemove extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveAtomic() throws Exception {
        putGetRemove(cacheConfiguration(ATOMIC, ONHEAP_TIERED, 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveAtomicOffheap() throws Exception {
        putGetRemove(cacheConfiguration(ATOMIC, OFFHEAP_TIERED, 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveTx() throws Exception {
        putGetRemove(cacheConfiguration(TRANSACTIONAL, ONHEAP_TIERED, 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveTxOffheap() throws Exception {
        putGetRemove(cacheConfiguration(TRANSACTIONAL, OFFHEAP_TIERED, 1));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void putGetRemove(final CacheConfiguration ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            long stopTime = System.currentTimeMillis() + 30_000;

            int iter = 0;

            while (System.currentTimeMillis() < stopTime) {
                if (iter++ % 100 == 0)
                    log.info("Iteration: " + iter);

                final AtomicInteger idx = new AtomicInteger();

                final int KEYS = 10;

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int nodeIdx = idx.getAndIncrement() % NODES;

                        IgniteCache<Object, Object> cache = ignite(nodeIdx).cache(ccfg.getName());

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        for (int i = 0; i < 10; i++) {
                            for (int k = 0; k < KEYS; k++) {
                                switch (rnd.nextInt(3)) {
                                    case 0:
                                        cache.put(k, rnd.nextInt(10_000));

                                        break;

                                    case 1:
                                        cache.get(k);

                                        break;

                                    case 2:
                                        cache.remove(k);

                                        break;

                                    default:
                                        fail();
                                }
                            }
                        }

                        return null;
                    }
                }, NODES * 10, "update-thread");

                Affinity aff = ignite(0).affinity(ccfg.getName());

                for (int k = 0; k < KEYS; k++) {
                    Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(k);

                    Object expVal = grid(nodes.iterator().next()).cache(ccfg.getName()).get(k);

                    for (int n = 0; n < NODES; n++) {
                        Ignite ignite = ignite(n);

                        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

                        if (nodes.contains(ignite.cluster().localNode()))
                            assertEquals(expVal, cache.localPeek(k));
                        else {
                            assertNull(cache.localPeek(k));
                            assertEquals(expVal, cache.get(k));
                        }
                    }
                }
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode,
        int backups) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        return ccfg;
    }
}
