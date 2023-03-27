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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Test for query with BinaryMarshaller and different serialization modes and with reflective serializer.
 */
public class CacheTxNotAllowReadFromBackupTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 2;

    /** */
    private static final int KEYS = 1000;

    /** */
    private static final int BATCH_SIZE = 10;

    /** */
    private static final int ITERATION_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyReplicated() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyReplicatedFullSync() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyPartitioned() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(NODES - 1);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyPartitionedFullSync() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(NODES - 1);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistency(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyReplicatedMvcc() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyReplicatedFullSyncMvcc() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyPartitionedMvcc() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(NODES - 1);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBackupConsistencyPartitionedFullSyncMvcc() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("test-cache");

        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(NODES - 1);
        cfg.setReadFromBackup(false);

        checkBackupConsistency(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        checkBackupConsistencyGetAll(cfg, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkBackupConsistency(CacheConfiguration<Integer, Integer> ccfg, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(ccfg);

        int nodeIdx = ThreadLocalRandom.current().nextInt(NODES);

        try {
            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Iteration: " + i);

                // Put data in one transaction.
                try (Transaction tx = grid(nodeIdx).transactions().txStart(txConcurrency, txIsolation)) {
                    for (int key = 0; key < KEYS; key++)
                        cache.put(key, key);

                    tx.commit();
                }

                int missCnt = 0;

                // Try to load data from another transaction.
                try (Transaction tx = grid(nodeIdx).transactions().txStart(txConcurrency, txIsolation)) {
                    for (int key = 0; key < KEYS; key++)
                        if (cache.get(key) == null)
                            ++missCnt;

                    tx.commit();
                }

                assertEquals("Failed. Found missing get()", 0, missCnt);
            }
        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkBackupConsistencyGetAll(CacheConfiguration<Integer, Integer> ccfg,
        TransactionConcurrency txConcurrency, TransactionIsolation txIsolation) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(ccfg);

        int nodeIdx = ThreadLocalRandom.current().nextInt(NODES);

        try {
            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Iteration: " + i);

                List<Set<Integer>> batches = createBatches();

                // Put data in one transaction.
                try (Transaction tx = grid(nodeIdx).transactions().txStart(txConcurrency, txIsolation)) {
                    for (int key = 0; key < KEYS; key++)
                        cache.put(key, key);

                    tx.commit();
                }

                // Try to load data from another transaction.
                try (Transaction tx = grid(nodeIdx).transactions().txStart(txConcurrency, txIsolation)) {
                    for (Set<Integer> batch : batches)
                        assertEquals("Failed. Found missing entries.", batch.size(), cache.getAll(batch).size());

                    tx.commit();
                }
            }
        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @return Batches.
     */
    @NotNull private List<Set<Integer>> createBatches() {
        List<Set<Integer>> batches = new ArrayList<>(KEYS / BATCH_SIZE + 1);

        int size = BATCH_SIZE;
        Set<Integer> batch = new HashSet<>();

        for (int key = 0; key < KEYS; key++) {
            batch.add(key);

            if (--size == 0) {
                size = BATCH_SIZE;
                batch = new HashSet<>();
                batches.add(batch);
            }
        }
        return batches;
    }
}
