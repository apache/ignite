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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheEntryProcessorExternalizableFailedTest extends GridCommonAbstractTest {
    /** */
    private static final int EXPECTED_VALUE = 42;

    /** */
    private static final int WRONG_VALUE = -1;

    /** */
    private static final int NODES = 3;

    /** */
    public static final int ITERATION_CNT = 1;

    /** */
    public static final int KEY = 10;

    /** */
    private boolean failOnWrite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        startClientGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failOnWrite = false;
    }

    /**
     * @return Server nodes.
     */
    private int getServerNodeCount() {
        return NODES;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimistic() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticOnePhaseCommit() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticOnePhaseCommitWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticOnePhaseCommitFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticOnePhaseCommitFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticOnePhaseCommit() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticOnePhaseCommitWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticOnePhaseCommitFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticOnePhaseCommitFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimistic() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccPessimisticOnePhaseCommit() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1).setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccPessimisticOnePhaseCommitWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1).setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccPessimisticOnePhaseCommitFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1).setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccPessimisticOnePhaseCommitFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1).setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccPessimistic() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2).setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccPessimisticWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2).setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccPessimisticFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2).setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccPessimisticFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2).setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setNearConfiguration(new NearCacheConfiguration<>());

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        failOnWrite = true;

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void doTestInvokeTest(CacheConfiguration ccfg, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        IgniteEx cln = grid(getServerNodeCount());

        grid(0).createCache(ccfg);

        IgniteCache clnCache;

        if (ccfg.getNearConfiguration() != null)
            clnCache = cln.createNearCache(ccfg.getName(), ccfg.getNearConfiguration());
        else
            clnCache = cln.cache(ccfg.getName());

        clnCache.put(KEY, EXPECTED_VALUE);

        try {
            // Explicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
                    checkExplicitMvccInvoke(cln, clnCache, txConcurrency, txIsolation);
                else
                    checkExplicitTxInvoke(cln, clnCache, txConcurrency, txIsolation);

                assertNull(cln.transactions().tx());

                assertEquals(EXPECTED_VALUE, clnCache.get(KEY));
            }

            // From affinity node.
            Ignite grid = grid(ThreadLocalRandom.current().nextInt(NODES));

            final IgniteCache cache = grid.cache(ccfg.getName());

            // Explicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
                    checkExplicitMvccInvoke(cln, clnCache, txConcurrency, txIsolation);
                else
                    checkExplicitTxInvoke(cln, clnCache, txConcurrency, txIsolation);

                assertNull(cln.transactions().tx());

                assertEquals(EXPECTED_VALUE, cache.get(KEY));
            }

            final IgniteCache clnCache0 = clnCache;

            // Implicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                //noinspection ThrowableNotThrown
                GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        clnCache0.invoke(KEY, createEntryProcessor());

                        return null;
                    }
                }, UnsupportedOperationException.class);

                assertNull(cln.transactions().tx());
            }

            assertEquals(EXPECTED_VALUE, clnCache.get(KEY));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param node Ignite node.
     * @param cache Node cache.
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation TransactionIsolation.
     */
    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    private void checkExplicitTxInvoke(Ignite node, IgniteCache cache, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) {
        try (final Transaction tx = node.transactions().txStart(txConcurrency, txIsolation)) {
            cache.put(KEY, WRONG_VALUE);

            cache.invoke(KEY, createEntryProcessor());

            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    tx.commit();

                    return null;
                }
            }, UnsupportedOperationException.class);
        }
    }

    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    private void checkExplicitMvccInvoke(Ignite node, IgniteCache cache, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) {
        try (final Transaction tx = node.transactions().txStart(txConcurrency, txIsolation)) {
            cache.put(KEY, WRONG_VALUE);

            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    cache.invoke(KEY, createEntryProcessor());

                    fail("Should never happened.");

                    tx.commit();

                    return null;
                }
            }, UnsupportedOperationException.class);
        }
    }

    /**
     * @return Entry processor.
     */
    private @NotNull EntryProcessor<Integer, Integer, Integer> createEntryProcessor() {
        return failOnWrite ? new ExternalizableFailedWriteEntryProcessor() :
            new ExternalizableFailedReadEntryProcessor();
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheConfiguration(CacheWriteSynchronizationMode wrMode, int backup) {
        return new CacheConfiguration("test-cache-" + wrMode + "-" + backup)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(backup);
    }

    /**
     *
     */
    private static class ExternalizableFailedWriteEntryProcessor implements EntryProcessor<Integer, Integer, Integer>,
        Externalizable {
        /** */
        public ExternalizableFailedWriteEntryProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(42);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /**
     *
     */
    private static class ExternalizableFailedReadEntryProcessor implements EntryProcessor<Integer, Integer, Integer>,
        Externalizable {
        /** */
        public ExternalizableFailedReadEntryProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(42);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException();
        }
    }
}
