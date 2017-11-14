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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_COMPLETED_TX_COUNT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheMissingCommitVersionSelfTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean putFailed;

    /** */
    private String maxCompletedTxCnt;

    /**
     */
    public GridCacheMissingCommitVersionSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        maxCompletedTxCnt = System.getProperty(IGNITE_MAX_COMPLETED_TX_COUNT);

        System.setProperty(IGNITE_MAX_COMPLETED_TX_COUNT, String.valueOf(5));

        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.setProperty(IGNITE_MAX_COMPLETED_TX_COUNT, maxCompletedTxCnt != null ? maxCompletedTxCnt : "");

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissingCommitVersion() throws Exception {
        final IgniteCache<Integer, Integer> cache = jcache();

        final int KEYS_PER_THREAD = 10_000;

        final AtomicInteger keyStart = new AtomicInteger();

        final ConcurrentLinkedDeque<Integer> q = new ConcurrentLinkedDeque<>();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int start = keyStart.getAndAdd(KEYS_PER_THREAD);

                for (int i = 0; i < KEYS_PER_THREAD && !putFailed; i++) {
                    int key = start + i;

                    try {
                        cache.put(key, 1);
                    }
                    catch (Exception e) {
                        log.info("Put failed [err=" + e + ", i=" + i + ']');

                        putFailed = true;

                        q.add(key);
                    }
                }

                return null;
            }
        }, 10, "put-thread");

        assertTrue("Test failed to provoke 'missing commit version' error.", putFailed);

        for (Integer key : q) {
            log.info("Trying to update " + key);

            IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

            asyncCache.put(key, 2);

            IgniteFuture<?> fut = asyncCache.future();

            try {
                fut.get(5000);
            }
            catch (IgniteFutureTimeoutException ignore) {
                fail("Put failed to finish in 5s: " + key);
            }
        }
    }
}
