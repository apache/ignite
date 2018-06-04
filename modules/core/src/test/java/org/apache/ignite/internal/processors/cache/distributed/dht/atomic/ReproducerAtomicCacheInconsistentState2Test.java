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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Reproducer for atomic cache inconsistency state.
 */
@SuppressWarnings("ErrorNotRethrown")
public class ReproducerAtomicCacheInconsistentState2Test extends GridCommonAbstractTest {
    /** */
    public void testWriteFullAsync() {
        Integer key = 100;

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(key, 0);

        Affinity<Integer> aff = affinity(cache);

        singleCommunicationFail(key, aff);

        cache.put(key, 5);

        doSleep(2_000);

        cache = cacheFromBackupNode(key, aff);

        assertNotNull(cache);

        assertEquals(5, (int)cache.get(key));
    }

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Grid count. */
    private int gridCnt = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setNetworkSendRetryCount(1);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER).setForceServerMode(true));

        CacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_ASYNC);
        ccfg.setRebalanceMode(SYNC);

        ccfg.setReadFromBackup(true);

        return ccfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCnt);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param key Key.
     * @param aff Aff.
     */
    private IgniteCache<Integer, Integer> cacheFromBackupNode(Integer key, Affinity<Integer> aff) {
        for (int i = 0; i < gridCnt; i++) {
            if (aff.isBackup(grid(i).localNode(), key))
                return grid(i).cache(DEFAULT_CACHE_NAME);
        }
        return null;
    }

    /**
     * @param key Key.
     * @param aff Aff.
     */
    private void singleCommunicationFail(Integer key, Affinity<Integer> aff) {
        for (int i = 0; i < gridCnt; i++) {
            if (aff.isPrimary(grid(i).localNode(), key)) {
                TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();
                spi.fail = true;
            }
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile boolean fail = false;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (fail && msg0 instanceof GridDhtAtomicAbstractUpdateRequest) {
                    fail = false;
                    throw new IgniteSpiException("Test error");
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

}
