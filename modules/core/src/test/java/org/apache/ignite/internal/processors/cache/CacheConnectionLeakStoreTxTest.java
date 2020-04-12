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

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.cache.TestCacheSession;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheConnectionLeakStoreTxTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final int CLIENT_NODE = 1;

    /** */
    private static volatile boolean isLoadFromStore;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        startClientGrid(CLIENT_NODE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        isLoadFromStore = false;
        TestStore.sessions.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupAtomic() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.ATOMIC, null, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupAtomicLoadFromStore() throws Exception {
        isLoadFromStore = true;

        checkConnectionLeak(CacheAtomicityMode.ATOMIC, null, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupOptimisticRepeatableRead() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupOptimisticRepeatableReadLoadFromStore() throws Exception {
        isLoadFromStore = true;

        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupOptimisticReadCommitted() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupOptimisticReadCommittedLoadFromStore() throws Exception {
        isLoadFromStore = true;

        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupPessimisticRepeatableRead() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupPessimisticReadCommitted() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionLeakOneBackupPessimisticReadCommittedLoadFromStore() throws Exception {
        isLoadFromStore = true;

        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582")
    @Test
    public void testConnectionLeakOneBackupMvccPessimisticRepeatableRead() throws Exception {
        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582")
    @Test
    public void testConnectionLeakOneBackupMvccPessimisticRepeatableReadLoadFromStore() throws Exception {
        isLoadFromStore = true;

        checkConnectionLeak(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation Transaction isolation.
     *
     * @throws Exception If failed.
     */
    private void checkConnectionLeak(
            CacheAtomicityMode atomicityMode,
            TransactionConcurrency txConcurrency,
            TransactionIsolation txIsolation
    ) throws Exception {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(atomicityMode);
        cacheCfg.setCacheStoreFactory(new TestStoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setLoadPreviousValue(true);

        Ignite ignite = ignite(CLIENT_NODE);
        IgniteCache<Integer, Integer> cache = ignite.createCache(cacheCfg);

        try {
            assertEquals(0, cache.size());

            if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL) {
                try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation)) {
                    cacheOp(cache);

                    tx.commit();
                }
            }
            else {
                cacheOp(cache);
            }

            assertTrue("Session was leak on nodes: " + TestStore.sessions, TestStore.sessions.isEmpty());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @param cache Cache.
     */
    private void cacheOp(IgniteCache<Integer, Integer> cache) {
        boolean b = cache.putIfAbsent(42, 42);

        log.info("PutIfAbsent: " + b);

        Integer val = cache.get(42);

        log.info("Get: " + val);
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStoreAdapter<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStoreAdapter<Integer, Integer> create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Integer> implements Serializable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** */
        private CacheStoreSession NULL = new TestCacheSession();

        /** */
        public static ConcurrentHashMap<CacheStoreSession, ClusterNode> sessions = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            addSession();

            return isLoadFromStore ? key : null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> e) throws CacheWriterException {
            addSession();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            addSession();
        }

        /**  */
        private void addSession() {
            sessions.put(ses == null ? NULL : ses, ignite.cluster().localNode());
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            sessions.remove(ses == null ? NULL : ses);
        }
    }
}
