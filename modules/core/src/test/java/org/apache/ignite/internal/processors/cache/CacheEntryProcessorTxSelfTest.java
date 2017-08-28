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

import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Test for value copy in entry processor.
 */
public class CacheEntryProcessorTxSelfTest extends GridCommonAbstractTest {

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NUM_SERVER_NODES = 2;

    /** The number of backups for {@link CacheAtomicityMode#TRANSACTIONAL}. */
    private static final int BACKUPS = 1;

    /** */
    private Ignite clientNode;

    /**
     * Returns the client node index.
     */
    private static int clientIndex() {
        return NUM_SERVER_NODES;
    }

    /**
     * Returns whether the node with the specified grid name
     * is a client node.
     *
     * @param gridName Grid name.
     * @return {@code true} if the node is a client node, otherwise {@code false}.
     */
    private static boolean isClient(String gridName) {
        return gridName.contains(Integer.toString(clientIndex()));
    }

    /**
     * Returns a cache configuration with the specified parameters.
     *
     * @param cacheName Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param backups The number of backups if cache atomicity mode is {@link CacheAtomicityMode#TRANSACTIONAL}.
     * @return Cache configuration.
     */
    private static <K, V> CacheConfiguration<K, V> getCacheConfiguration(
        String cacheName, CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups
    ) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);

        if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Start server nodes.
        startGridsMultiThreaded(NUM_SERVER_NODES);

        // Start a client node.
        clientNode = startGrid(clientIndex());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (isClient(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedPessimisticReadCommitted() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedPessimisticReadCommitted"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.READ_COMMITTED
        );
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedPessimisticRepeatableRead() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedPessimisticRepeatableRead"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ
        );
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedOptimisticReadCommitted() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedOptimisticReadCommitted"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.READ_COMMITTED
        );
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedOptimisticRepeatableRead() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedOptimisticRepeatableRead"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.REPEATABLE_READ
        );
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedOptimisticSerializable() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedOptimisticSerializable"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.SERIALIZABLE
        );
    }
    
    /** */
    public void testGetAfterInvokeWithCreatePartitionedPessimisticReadCommitted() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedPessimisticReadCommitted"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.READ_COMMITTED
        );
    }

    /** */
    public void testGetAfterInvokeWithCreatePartitionedPessimisticRepeatableRead() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedPessimisticRepeatableRead"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ
        );
    }

    /** */
    public void testGetAfterInvokeWithCreatePartitionedOptimisticReadCommitted() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedOptimisticReadCommitted"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.READ_COMMITTED
        );
    }

    /** */
    public void testGetAfterInvokeWithCreatePartitionedOptimisticRepeatableRead() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedOptimisticRepeatableRead"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.REPEATABLE_READ
        );
    }

    /** */
    public void testGetAfterInvokeWithCreatePartitionedOptimisticSerializable() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedOptimisticSerializable"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.TRANSACTIONAL,
            TransactionConcurrency.OPTIMISTIC,
            TransactionIsolation.SERIALIZABLE
        );
    }

    /** */
    public void testGetAfterInvokeWithCreateReplicatedAtomic() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("replicatedAtomic"),
            CacheMode.REPLICATED,
            CacheAtomicityMode.ATOMIC
        );
    }

    /** */
    public void testGetAfterInvokeWithCreatePartitionedAtomic() {
        getAfterInvokeWithCreate(
            GridTestUtils.createRandomizedName("partitionedAtomic"),
            CacheMode.PARTITIONED,
            CacheAtomicityMode.ATOMIC
        );
    }

    /**
     * Call the get operation after an EntryProcessor invocation which creates a new value.
     *
     * @param cacheName Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param transactionConcurrency Transaction concurrency mode.
     * @param transactionIsolation Transaction isolation level.
     */
    private void getAfterInvokeWithCreate(
        String cacheName,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation
    ) {
        IgniteCache<Long, String> cache = clientNode.createCache(
            CacheEntryProcessorTxSelfTest.<Long, String>getCacheConfiguration(
                cacheName, cacheMode, atomicityMode, BACKUPS
            )
        );

        IgniteTransactions txns = clientNode.transactions();
        final Long key = 1L;
        final String newVal = "CREATED";
        boolean txInfoProvided = transactionConcurrency != null && transactionIsolation != null;

        try (
            Transaction tx =
                txInfoProvided ? txns.txStart(transactionConcurrency, transactionIsolation) : txns.txStart()
        ) {
            String entryProcRes = cache.invoke(key, new CacheEntryProcessor<Long, String, String>() {
                /** */
                private static final long serialVersionUID = 1L;

                @Override public String process(MutableEntry<Long, String> entry, Object... args) {
                    entry.setValue(newVal);

                    return null;
                }
            });

            String gottenVal = cache.get(key);
            assertEquals(newVal, gottenVal);

            tx.commit();
        }

        assertEquals(newVal, cache.get(key));
    }

    /**
     * Call the get operation after an EntryProcessor invocation which creates a new value.
     *
     * @param cacheName Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     */
    private void getAfterInvokeWithCreate(
        String cacheName,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode
    ) {
        getAfterInvokeWithCreate(cacheName, cacheMode, atomicityMode, null, null);
    }
}
