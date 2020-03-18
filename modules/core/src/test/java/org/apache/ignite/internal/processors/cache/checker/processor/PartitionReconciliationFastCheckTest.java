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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.SCHEDULED;

/**
 * Test fast-check mode of partition reconciliation utility.
 */
public class PartitionReconciliationFastCheckTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 3;

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 12));
        ccfg.setBackups(NODES_CNT - 1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        cfg.setAutoActivationEnabled(false);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that partition reconciliation utility does nothing if the last PME did not report invalid partitions.
     *
     * @throws Exception If failed.
     */
    public void testAbsenceOfInvalidPartitions() throws Exception {
        final AtomicInteger batchCnt = new AtomicInteger();

        // Count all planned batches.
        ReconciliationEventListener evtsLsnr = (stage, workload) -> {
            if (stage == SCHEDULED && workload instanceof Batch)
                batchCnt.incrementAndGet();
        };

        // There is no need to corrupt data.
        Runnable emptySimulator = () -> {};

        ReconciliationResult res = fastCheckTest(evtsLsnr, emptySimulator);

        assertTrue(
            "Number of inconsistent keys should be equal to 0.",
            res.partitionReconciliationResult().isEmpty());

        assertEquals("Number of scheduled validations of partitions.", batchCnt.get(), 0);
    }

    /**
     * Tests that partition reconciliation utility only checks partitions with different values of update counter.
     *
     * @throws Exception If failed.
     */
    public void testPartitionsWithBrokenUpdateCounters() throws Exception {
        final Map<Integer, Integer> partMap = new ConcurrentHashMap<>();

        // Count all planned batches.
        ReconciliationEventListener evtsLsnr = (stage, workload) -> {
            if (stage == SCHEDULED && workload instanceof Batch) {
                Batch batch = (Batch)workload;

                partMap.put(batch.partitionId(), batch.partitionId());
            }
        };

        Integer key = 12;

        Runnable updCntrModifier = () -> {
            corruptDataEntry(
                grid(2).cachex(DEFAULT_CACHE_NAME).context(),
                key,
                true,
                false,
                new GridCacheVersion(0, 0, 0L),
                "_broken");
        };

        ReconciliationResult res = fastCheckTest(evtsLsnr, updCntrModifier);

        assertEquals(
            "Number of inconsistent keys should not be empty.",
            1,
            res.partitionReconciliationResult().inconsistentKeysCount()
        );

        // need to check, that all workloads relate to the same partition.
        assertEquals("Only one partition should be checked.", 1, partMap.size());

        assertEquals(
            "Wrong partition identifier.",
            grid(0).affinity(DEFAULT_CACHE_NAME).partition(key),
            partMap.values().iterator().next().intValue());
    }

    /**
     * Tests that partition reconciliation utility only checks partitions with different sizes.
     *
     * @throws Exception If failed.
     */
    public void testPartitionsWithBrokenSize() throws Exception {
        final Map<Integer, Integer> partMap = new ConcurrentHashMap<>();

        // Count all planned batches.
        ReconciliationEventListener evtsLsnr = (stage, workload) -> {
            if (stage == SCHEDULED && workload instanceof Batch) {
                Batch batch = (Batch)workload;

                partMap.put(batch.partitionId(), batch.partitionId());
            }
        };

        Integer key = 12;

        Runnable partSizeModifier = () ->
            simulateMissingEntryCorruption(grid(2).cachex(DEFAULT_CACHE_NAME).context(), key);

        ReconciliationResult res = fastCheckTest(evtsLsnr, partSizeModifier);

        assertEquals(
            "Number of inconsistent keys should not be empty.",
            1,
            res.partitionReconciliationResult().inconsistentKeysCount()
        );

        // need to check, that all workloads relate to the same partition.
        assertEquals("Only one partition should be checked.", 1, partMap.size());

        assertEquals(
            "Wrong partition identifier.",
            grid(0).affinity(DEFAULT_CACHE_NAME).partition(key),
            partMap.values().iterator().next().intValue());
    }

    /**
     * Tests fast-check mode of partition reconciliation utility.
     *
     * @param lsnr Reconciliation events listener.
     * @param inconsistencySimulator Callback that is used to simulate data inconsistency.
     */
    private ReconciliationResult fastCheckTest(
        ReconciliationEventListener lsnr,
        Runnable inconsistencySimulator
    ) throws Exception {
        // Start cluster and fill the cache.
        startGrids(NODES_CNT);

        grid(0).cluster().active(true);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        // Simulate data inconsistency.
        inconsistencySimulator.run();

        ReconciliationResult res;

        try {
            // Block rebalancing
            for (Ignite ignite : G.allGrids()) {
                TestRecordingCommunicationSpi.spi(ignite)
                    .blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);
            }

            ReconciliationEventListenerProvider.defaultListenerInstance(lsnr);

            // Starting this node should trigger partitions validation.
            startGrid(NODES_CNT);

            res = partitionReconciliation(
                grid(0),
                new VisorPartitionReconciliationTaskArg.Builder()
                    .fastCheck(true)
                    .repair(true)
                    .repairAlg(RepairAlgorithm.LATEST));
        }
        finally {
            ReconciliationEventListenerProvider.defaultListenerInstance((stage, workload) -> {});

            // Unblock rebalancing.
            for (Ignite ignite : G.allGrids())
                TestRecordingCommunicationSpi.spi(ignite).stopBlock();
        }

        awaitPartitionMapExchange(false, true, null);

        assertFalse(idleVerify(grid(0), DEFAULT_CACHE_NAME).hasConflicts());

        return res;
    }
}
