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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.event.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class CacheContinuousQueryFailoverAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @throws Exception If failed.
     */
    public void testBackupQueue() throws Exception {
        final int SRV_NODES = 4;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(null);

        CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setAutoUnsubscribe(true);

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        int PARTS = 1;

        for (int i = 0; i < SRV_NODES - 1; i++) {
            log.info("Stop iteration: " + i);

            TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            Ignite ignite = ignite(i);

            IgniteCache<Object, Object> cache = ignite.cache(null);

            List<Integer> keys = testKeys(cache, PARTS);

            lsnr.latch = new CountDownLatch(keys.size());

            boolean first = true;

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ']');

                cache.put(key, key);

                if (first) {
                    spi.skipMsg = true;

                    first = false;
                }
            }

            stopGrid(i);

            assertTrue("Failed to wait for notifications", lsnr.latch.await(5, SECONDS));

            lsnr.latch = null;

            awaitPartitionMapExchange();
        }

        for (int i = 0; i < SRV_NODES - 1; i++) {
            log.info("Start iteration: " + i);

            Ignite ignite = startGrid(i);

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = ignite.cache(null);

            List<Integer> keys = testKeys(cache, PARTS);

            lsnr.latch = new CountDownLatch(keys.size());

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ']');

                cache.put(key, key);
            }

            if (!lsnr.latch.await(5, SECONDS))
                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

            lsnr.latch = null;
        }

        cur.close();
    }

    /**
     * @param cache Cache.
     * @param parts Number of partitions.
     * @return Keys.
     */
    private List<Integer> testKeys(IgniteCache<Object, Object> cache, int parts) {
        Ignite ignite = cache.unwrap(Ignite.class);

        List<Integer> res = new ArrayList<>();

        Affinity<Object> aff = ignite.affinity(cache.getName());

        ClusterNode node = ignite.cluster().localNode();

        int[] nodeParts = aff.primaryPartitions(node);

        final int KEYS_PER_PART = 1;

        for (int i = 0; i < parts; i++) {
            int part = nodeParts[i];

            int cnt = 0;

            for (int key = 0; key < 100_000; key++) {
                if (aff.partition(key) == part && aff.isPrimary(node, key)) {
                    res.add(key);

                    if (++cnt == KEYS_PER_PART)
                        break;
                }
            }

            assertEquals(KEYS_PER_PART, cnt);
        }

        assertEquals(parts * KEYS_PER_PART, res.size());

        return res;
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        private volatile CountDownLatch latch;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts) {
                ignite.log().info("Received cache event: " + evt);

                CountDownLatch latch = this.latch;

                assertTrue(latch != null);
                assertTrue(latch.getCount() > 0);

                latch.countDown();
            }
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private volatile boolean skipMsg;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (skipMsg && msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridContinuousMessage) {
                    log.info("Skip continuous message: " + msg0);

                    return;
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
