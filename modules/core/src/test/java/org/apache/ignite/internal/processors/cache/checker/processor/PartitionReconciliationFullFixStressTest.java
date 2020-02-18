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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.REMOVE;

/**
 * Tests the utility under loading.
 */
public class PartitionReconciliationFullFixStressTest extends PartitionReconciliationStressTest {
    /**
     * Makes different variations of input params.
     */
    @Parameterized.Parameters(
        name = "atomicity = {0}, partitions = {1}, fixModeEnabled = {2}, repairAlgorithm = {3}, parallelism = {4}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        int[] partitions = {1, 32};
        RepairAlgorithm[] repairAlgorithms = {LATEST, PRIMARY, MAJORITY, REMOVE};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            for (int parts : partitions)
                for (RepairAlgorithm repairAlgorithm : repairAlgorithms)
                    params.add(new Object[] {atomicityMode, parts, true, repairAlgorithm, 4});
        }

        params.add(new Object[] {CacheAtomicityMode.ATOMIC, 1, true, MAJORITY, 1});
        params.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, 32, true, REMOVE, 1});

        return params;
    }

    /**
     * Test #38 Maximum stress test with -fix and every key is corrupted
     *
     * @throws Exception If failed.
     */
    @Override @Test
    public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        IgniteCache<Integer, String> clientCache = client.cache(DEFAULT_CACHE_NAME);

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(DEFAULT_CACHE_NAME).context();

        Set<Integer> corruptedKeys = new HashSet<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            clientCache.put(i, String.valueOf(i));
            corruptedKeys.add(i);

            if (i % 3 == 0)
                simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], i);
            else
                simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], i);
        }

        AtomicBoolean stopRandomLoad = new AtomicBoolean(false);

        final Set<Integer>[] reloadedKeys = new Set[6];

        AtomicInteger threadCntr = new AtomicInteger(0);

        IgniteInternalFuture<Long> randLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            int threadId = threadCntr.incrementAndGet() - 1;
            reloadedKeys[threadId] = new HashSet<>();

            while (!stopRandomLoad.get()) {
                int i = ThreadLocalRandom.current().nextInt(KEYS_CNT);
                clientCache.put(i, String.valueOf(2 * i));
                reloadedKeys[threadId].add(i);
            }
        }, 6, "rand-loader");

        ReconciliationResult res = partitionReconciliation(ig, fixMode, repairAlgorithm, parallelism, DEFAULT_CACHE_NAME);

        log.info(">>>> Partition reconciliation finished");

        stopRandomLoad.set(true);

        randLoadFut.get();

        for (Set<Integer> reloadedKey : reloadedKeys)
            corruptedKeys.removeAll(reloadedKey);

        assertResultContainsConflictKeys(res, DEFAULT_CACHE_NAME, corruptedKeys);

        assertFalse(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts());
    }
}
