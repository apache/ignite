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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheSingleGetMessageTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        startClientGridsMultiThreaded(SRVS, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleGetMessage() throws Exception {
        checkSingleGetMessage(cacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7371")
    @Test
    public void testMvccSingleGetMessage() throws Exception {
        checkSingleGetMessage(mvccCacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    public void checkSingleGetMessage(List<CacheConfiguration<Integer, Integer>> ccfgs) throws Exception {
        assertFalse(ignite(0).configuration().isClientMode());
        assertTrue(ignite(SRVS).configuration().isClientMode());

        for (int i = 0; i < ccfgs.size(); i++) {
            CacheConfiguration<Integer, Integer> ccfg = ccfgs.get(i);

            ccfg.setName("cache-" + i);

            log.info("Test cache: " + i);

            ignite(0).createCache(ccfg);

            try {
                IgniteCache<Integer, Integer> srvCache = ignite(0).cache(ccfg.getName());
                IgniteCache<Integer, Integer> clientCache = ignite(SRVS).cache(ccfg.getName());

                Integer key = nearKey(clientCache);

                checkSingleGetMessage(clientCache, key, false);

                if (ccfg.getBackups() > 0) {
                    key = backupKeys(srvCache, 1, 100_000).get(0);

                    checkSingleGetMessage(srvCache, key, true);
                }

                if (ccfg.getCacheMode() != REPLICATED) {
                    key = nearKeys(srvCache, 1, 200_000).get(0);

                    checkSingleGetMessage(srvCache, key, false);
                }
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param backup {@code True} if given key is backup key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkSingleGetMessage(IgniteCache<Integer, Integer> cache,
        Integer key,
        boolean backup) throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cache.getConfiguration(CacheConfiguration.class);

        Ignite node = cache.unwrap(Ignite.class);

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();

        spi.record(GridNearSingleGetRequest.class);

        Ignite primary = primaryNode(key, cache.getName());

        assertNotSame(node, primary);

        TestRecordingCommunicationSpi primarySpi =
            (TestRecordingCommunicationSpi)primary.configuration().getCommunicationSpi();

        primarySpi.record(GridNearSingleGetResponse.class);

        assertNull(cache.get(key));

        if (backup)
            checkNoMessages(spi, primarySpi);
        else
            checkMessages(spi, primarySpi);

        assertFalse(cache.containsKey(key));

        if (backup)
            checkNoMessages(spi, primarySpi);
        else
            checkMessages(spi, primarySpi);

        cache.put(key, 1);

        assertNotNull(cache.get(key));

        if (backup)
            checkNoMessages(spi, primarySpi);
        else
            checkMessages(spi, primarySpi);

        assertTrue(cache.containsKey(key));

        if (backup)
            checkNoMessages(spi, primarySpi);
        else
            checkMessages(spi, primarySpi);

        if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
            cache.remove(key);

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertNull(cache.get(key));

                tx.commit();
            }

            if (backup)
                checkNoMessages(spi, primarySpi);
            else
                checkMessages(spi, primarySpi);

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertFalse(cache.containsKey(key));

                tx.commit();
            }

            if (backup)
                checkNoMessages(spi, primarySpi);
            else
                checkMessages(spi, primarySpi);

            cache.put(key, 1);

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertNotNull(cache.get(key));

                tx.commit();
            }

            if (backup)
                checkNoMessages(spi, primarySpi);
            else
                checkMessages(spi, primarySpi);

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertTrue(cache.containsKey(key));

                tx.commit();
            }

            if (backup)
                checkNoMessages(spi, primarySpi);
            else
                checkMessages(spi, primarySpi);
        }
    }

    /**
     * @param spi Near node SPI.
     * @param primarySpi Primary node SPI.
     */
    private void checkMessages(TestRecordingCommunicationSpi spi, TestRecordingCommunicationSpi primarySpi) {
        List<Object> msgs = spi.recordedMessages(false);

        assertEquals(1, msgs.size());
        assertTrue(msgs.get(0) instanceof GridNearSingleGetRequest);

        msgs = primarySpi.recordedMessages(false);

        assertEquals(1, msgs.size());
        assertTrue(msgs.get(0) instanceof GridNearSingleGetResponse);
    }

    /**
     * @param spi Near node SPI.
     * @param primarySpi Primary node SPI.
     */
    private void checkNoMessages(TestRecordingCommunicationSpi spi, TestRecordingCommunicationSpi primarySpi) {
        List<Object> msgs = spi.recordedMessages(false);
        assertEquals(0, msgs.size());

        msgs = primarySpi.recordedMessages(false);
        assertEquals(0, msgs.size());
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL, FULL_SYNC, 0));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL, FULL_SYNC, 1));
        ccfgs.add(cacheConfiguration(REPLICATED, TRANSACTIONAL, FULL_SYNC, 0));

        ccfgs.add(cacheConfiguration(PARTITIONED, ATOMIC, FULL_SYNC, 0));
        ccfgs.add(cacheConfiguration(PARTITIONED, ATOMIC, FULL_SYNC, 1));
        ccfgs.add(cacheConfiguration(REPLICATED, ATOMIC, FULL_SYNC, 0));

        return ccfgs;
    }

    /**
     * @return Mvcc cache configurations to test.
     */
    private List<CacheConfiguration<Integer, Integer>> mvccCacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, FULL_SYNC, 0));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, FULL_SYNC, 1));
        ccfgs.add(cacheConfiguration(REPLICATED, TRANSACTIONAL_SNAPSHOT, FULL_SYNC, 0));

        return ccfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode syncMode,
        int backups) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }
}
