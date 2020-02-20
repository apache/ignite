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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheCommitDelayTxRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** */
    private static volatile boolean commit;

    /** */
    private static volatile CountDownLatch commitStartedLatch;

    /** */
    private static volatile CountDownLatch commitFinishLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRecovery1() throws Exception {
        checkRecovery(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRecovery2() throws Exception {
        checkRecovery(2, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryStoreEnabled1() throws Exception {
        checkRecovery(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryStoreEnabled2() throws Exception {
        checkRecovery(2, true);
    }

    /**
     * @param backups Number of cache backups.
     * @param useStore If {@code true} tests cache with store configured.
     * @throws Exception If failed.
     */
    private void checkRecovery(int backups, boolean useStore) throws Exception {
        startGridsMultiThreaded(SRVS, false);

        Ignite clientNode = startClientGrid(SRVS);

        assertTrue(clientNode.configuration().isClientMode());

        clientNode.createCache(cacheConfiguration(backups, useStore));

        awaitPartitionMapExchange();

        Ignite srv = ignite(0);

        assertFalse(srv.configuration().isClientMode());

        for (Boolean pessimistic : Arrays.asList(false, true)) {
            checkRecovery(backupKey(srv.cache(DEFAULT_CACHE_NAME)), srv, pessimistic, useStore);

            checkRecovery(nearKey(srv.cache(DEFAULT_CACHE_NAME)), srv, pessimistic, useStore);

            checkRecovery(nearKey(clientNode.cache(DEFAULT_CACHE_NAME)), clientNode, pessimistic, useStore);

            srv = ignite(0);

            assertFalse(srv.configuration().isClientMode());
        }
    }

    /**
     * @param key Key.
     * @param ignite Node executing update.
     * @param pessimistic If {@code true} uses pessimistic transaction.
     * @param useStore {@code True} if store is used.
     * @throws Exception If failed.
     */
    private void checkRecovery(final Integer key,
        final Ignite ignite,
        final boolean pessimistic,
        final boolean useStore) throws Exception {
        Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);

        assertNotSame(ignite, primary);

        List<Ignite> backups = backupNodes(key, DEFAULT_CACHE_NAME);

        assertFalse(backups.isEmpty());

        final Set<String> backupNames = new HashSet<>();

        for (Ignite node : backups)
            backupNames.add(node.name());

        log.info("Check recovery [key=" + key +
            ", pessimistic=" + pessimistic +
            ", primary=" + primary.name() +
            ", backups=" + backupNames +
            ", node=" + ignite.name() + ']');

        final IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(key, 0);

        commitStartedLatch = new CountDownLatch(backupNames.size());
        commitFinishLatch = new CountDownLatch(1);

        commit = false;

        TestEntryProcessor.skipFirst = useStore ? ignite.name() : null;

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start update.");

                if (pessimistic) {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.invoke(key, new TestEntryProcessor(backupNames));

                        commit = true;

                        log.info("Start commit.");

                        assertEquals(backupNames.size(), commitStartedLatch.getCount());

                        tx.commit();
                    }
                }
                else {
                    commit = true;

                    cache.invoke(key, new TestEntryProcessor(backupNames));
                }

                log.info("End update, execute get.");

                Integer val = cache.get(key);

                log.info("Get value: " + val);

                assertEquals(1, (Object)val);

                return null;
            }
        }, "update-thread");

        assertTrue(commitStartedLatch.await(30, SECONDS));

        log.info("Stop node: " + primary.name());

        primary.close();

        commitFinishLatch.countDown();

        fut.get();

        for (Ignite node : G.allGrids())
            assertEquals(1, node.cache(DEFAULT_CACHE_NAME).get(key));

        cache.put(key, 2);

        for (Ignite node : G.allGrids())
            assertEquals(2, node.cache(DEFAULT_CACHE_NAME).get(key));

        startGrid(primary.name());

        for (Ignite node : G.allGrids())
            assertEquals(2, node.cache(DEFAULT_CACHE_NAME).get(key));

        cache.put(key, 3);

        for (Ignite node : G.allGrids())
            assertEquals(3, node.cache(DEFAULT_CACHE_NAME).get(key));

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Integer, Integer, Void> {
        /** */
        private Set<String> nodeNames;

        /** Skips first call for given node (used to skip call for store update). */
        private static String skipFirst;

        /**
         * @param nodeNames Node names where sleep will be called.
         */
        public TestEntryProcessor(Set<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            Ignite ignite = entry.unwrap(Ignite.class);

            System.out.println(Thread.currentThread().getName() + " process [node=" + ignite.name() +
                ", commit=" + commit + ", skipFirst=" + skipFirst + ']');

            boolean skip = false;

            if (commit && ignite.name().equals(skipFirst)) {
                skipFirst = null;

                skip = true;
            }

            if (!skip && commit && nodeNames.contains(ignite.name())) {
                try {
                    System.out.println(Thread.currentThread().getName() + " start process invoke.");

                    assertTrue(commitStartedLatch != null && commitStartedLatch.getCount() > 0);

                    commitStartedLatch.countDown();

                    assertTrue(commitFinishLatch.await(10, SECONDS));

                    System.out.println(Thread.currentThread().getName() + " end process invoke.");
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            else
                System.out.println(Thread.currentThread().getName() + " invoke set value.");

            entry.setValue(1);

            return null;
        }
    }

    /**
     * @param backups Number of backups.
     * @param useStore If {@code true} adds cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(int backups, boolean useStore) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        if (useStore) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());

            ccfg.setWriteThrough(true);
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter<Object, Object>() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
