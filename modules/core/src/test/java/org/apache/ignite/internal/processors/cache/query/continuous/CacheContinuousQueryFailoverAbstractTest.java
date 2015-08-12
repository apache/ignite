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
    private static volatile boolean err;

    /** */
    private boolean client;

    /** */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setIdleConnectionTimeout(100);

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        err = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
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
    public void testOneBackup() throws Exception {
        checkBackupQueue(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackups() throws Exception {
        checkBackupQueue(3);
    }

    /**
     * @param backups Number of backups.
     * @throws Exception If failed.
     */
    private void checkBackupQueue(int backups) throws Exception {
        this.backups = backups;

        final int SRV_NODES = 4;

        startGrids(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(null);

        assertEquals(backups, qryClientCache.getConfiguration(CacheConfiguration.class).getBackups());

        Affinity<Object> aff = qryClient.affinity(null);

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

            CountDownLatch latch = new CountDownLatch(keys.size());

            lsnr.latch = latch;

            boolean first = true;

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

                cache.put(key, key);

                if (first) {
                    spi.skipMsg = true;

                    first = false;
                }
            }

            stopGrid(i);

            if (!latch.await(5, SECONDS))
                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

            awaitPartitionMapExchange();
        }

        for (int i = 0; i < SRV_NODES - 1; i++) {
            log.info("Start iteration: " + i);

            Ignite ignite = startGrid(i);

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = ignite.cache(null);

            List<Integer> keys = testKeys(cache, PARTS);

            CountDownLatch latch = new CountDownLatch(keys.size());

            lsnr.latch = latch;

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

                cache.put(key, key);
            }

            if (!latch.await(5, SECONDS))
                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');
        }

        cur.close();

        assertFalse(err);
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

        final int KEYS_PER_PART = 2;

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
     * @throws Exception If failed.
     */
    public void testBackupQueueCleanup() throws Exception {
        startGrids(2);

        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);

        CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setAutoUnsubscribe(true);

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = ignite1.cache(null).query(qry);

        IgniteCache<Object, Object> cache0 = ignite0.cache(null);

        final int KEYS = 1;

        List<Integer> keys = primaryKeys(cache0, KEYS);

        CountDownLatch latch = new CountDownLatch(keys.size());

        lsnr.latch = latch;

        for (Integer key : keys)
            cache0.put(key, key);

        if (!latch.await(5, SECONDS))
            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

        Thread.sleep(10_000);

        cur.close();
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

                testAssert(evt, latch != null);
                testAssert(evt, latch.getCount() > 0);

                latch.countDown();

                if (latch.getCount() == 0)
                    this.latch = null;
            }
        }

        /**
         * @param evt Event.
         * @param cond Condition to check.
         */
        private void testAssert(CacheEntryEvent evt, boolean cond) {
            if (!cond) {
                ignite.log().info("Unexpected event: " + evt);

                err = true;
            }

            assertTrue(cond);
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
