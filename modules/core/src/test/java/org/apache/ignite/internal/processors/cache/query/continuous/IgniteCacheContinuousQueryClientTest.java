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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.CountDownLatch;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheContinuousQueryClientTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoins() throws Exception {
        startGrids(2);

        client = true;

        final int CLIENT_ID = 3;

        Ignite clientNode = startGrid(CLIENT_ID);

        client = false;

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = clientNode.cache(null).query(qry);

        for (int i = 0; i < 10; i++) {
            log.info("Start iteration: " + i);

            lsnr.latch = new CountDownLatch(1);

            Ignite joined1 = startGrid(4);

            IgniteCache<Object, Object> joinedCache1 = joined1.cache(null);

            joinedCache1.put(primaryKey(joinedCache1), 1);

            assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

            lsnr.latch = new CountDownLatch(1);

            Ignite joined2 = startGrid(5);

            IgniteCache<Object, Object> joinedCache2 = joined2.cache(null);

            joinedCache2.put(primaryKey(joinedCache2), 2);

            assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

            stopGrid(4);

            stopGrid(5);
        }

        cur.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinsRestartQuery() throws Exception {
        startGrids(2);

        client = true;

        final int CLIENT_ID = 3;

        Ignite clientNode = startGrid(CLIENT_ID);

        client = false;

        for (int i = 0; i < 10; i++) {
            log.info("Start iteration: " + i);

            final CacheEventListener lsnr = new CacheEventListener();

            ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

            qry.setLocalListener(lsnr);

            QueryCursor<?> cur = clientNode.cache(null).query(qry);

            lsnr.latch = new CountDownLatch(1);

            Ignite joined1 = startGrid(4);

            IgniteCache<Object, Object> joinedCache1 = joined1.cache(null);

            joinedCache1.put(primaryKey(joinedCache1), 1);

            assertTrue("Failed to wait for event.", lsnr.latch.await(5, SECONDS));

            cur.close();

            lsnr.latch = new CountDownLatch(1);

            Ignite joined2 = startGrid(5);

            IgniteCache<Object, Object> joinedCache2 = joined2.cache(null);

            joinedCache2.put(primaryKey(joinedCache2), 2);

            assertFalse("Unexpected event received.", GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return 1 != lsnr.latch.getCount();
                }
            }, 1000));

            stopGrid(4);

            stopGrid(5);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeLeft() throws Exception {
        startGrids(3);

        client = true;

        final int CLIENT_ID = 3;

        Ignite clnNode = startGrid(CLIENT_ID);

        client = false;

        IgniteOutClosure<IgniteCache<Integer, Integer>> rndCache =
            new IgniteOutClosure<IgniteCache<Integer, Integer>>() {
                int cnt = 0;

                @Override public IgniteCache<Integer, Integer> apply() {
                    ++cnt;

                    return grid(CLIENT_ID).cache(null);
                }
            };

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = clnNode.cache(null).query(qry);

        boolean first = true;

        int keyCnt = 1;

        for (int i = 0; i < 10; i++) {
            log.info("Start iteration: " + i);

            if (first)
                first = false;
            else {
                for (int srv = 0; srv < CLIENT_ID - 1; srv++)
                    startGrid(srv);
            }

            lsnr.latch = new CountDownLatch(keyCnt);

            for (int key = 0; key < keyCnt; key++)
                rndCache.apply().put(key, key);

            assertTrue("Failed to wait for event. Left events: " + lsnr.latch.getCount(),
                lsnr.latch.await(10, SECONDS));

            for (int srv = 0; srv < CLIENT_ID - 1; srv++)
                stopGrid(srv);
        }

        cur.close();
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        private volatile CountDownLatch latch = new CountDownLatch(1);

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts) {
                log.info("Received cache event: " + evt);

                latch.countDown();
            }
        }
    }
}