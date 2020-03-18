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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.READY;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationRecheckAttemptsTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Wait to start first last recheck. */
    protected static  volatile CountDownLatch waitToStartFirstLastRecheck;

    /** Wait key reporation. */
    protected static volatile CountDownLatch waitKeyReporation;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        ccfg.setBackups(NODES_CNT - 1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);
        cfg.setActiveOnStart(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that only one call happened.
     */
    @Test
    public void testZeroAttemptMakeOnlyOneRecheck() {
        testRecheckCount(0);
    }

    /**
     * Checks that three additional calls happened.
     */
    @Test
    public void testThreeAdditionalAttempts() {
        testRecheckCount(3);
    }

    /**
     * Check that broken keys are excluded if they are repaired.
     */
    @Test
    public void testBrokenKeysWillFixedDuringRecheck() throws InterruptedException, IgniteInterruptedCheckedException {
        final ConcurrentMap<UUID, AtomicInteger> recheckAttempts = new ConcurrentHashMap<>();

        waitToStartFirstLastRecheck = new CountDownLatch(1);
        waitKeyReporation = new CountDownLatch(1);

        ReconciliationEventListenerProvider.defaultListenerInstance((stage, workload) -> {
            if (stage.equals(READY) && workload instanceof RecheckRequest) {
                int attempt = recheckAttempts.computeIfAbsent(workload.workloadChainId(), (key) -> new AtomicInteger(0)).incrementAndGet();

                if (attempt == 2)
                    waitToStartFirstLastRecheck.countDown();
            }

            if (waitToStartFirstLastRecheck.getCount() == 0) {
                try {
                    waitKeyReporation.await();
                }
                catch (InterruptedException ignore) {
                }
            }
        });

        for (int i = 0; i < 15; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

            simulateOutdatedVersionCorruption(grid(0).cachex(DEFAULT_CACHE_NAME).context(), i);
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(false);
        builder.parallelism(1);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.recheckAttempts(3);
        builder.recheckDelay(0);

        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        waitToStartFirstLastRecheck.await();

        for (int i = 0; i < 15; i++) // repair keys
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

        waitKeyReporation.countDown();

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
    }

    /**
     *
     */
    private void testRecheckCount(int attempts) {
        final ConcurrentMap<UUID, AtomicInteger> recheckAttempts = new ConcurrentHashMap<>();

        ReconciliationEventListenerProvider.defaultListenerInstance((stage, workload) -> {
            if (stage.equals(FINISHED) && workload instanceof RecheckRequest)
                recheckAttempts.computeIfAbsent(workload.workloadChainId(), (key) -> new AtomicInteger(0)).incrementAndGet();
        });

        for (int i = 0; i < 15; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

            simulateOutdatedVersionCorruption(grid(0).cachex(DEFAULT_CACHE_NAME).context(), i);
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(false);
        builder.parallelism(1);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.recheckAttempts(attempts);
        builder.recheckDelay(0);

        ReconciliationResult res = partitionReconciliation(client, builder);

        assertEquals(15, res.partitionReconciliationResult().inconsistentKeysCount());

        for (Map.Entry<UUID, AtomicInteger> entry : recheckAttempts.entrySet())
            assertEquals("Session: " + entry.getKey() + " has wrong value: " + entry.getValue().get(), 1 + attempts, entry.getValue().get());
    }
}
