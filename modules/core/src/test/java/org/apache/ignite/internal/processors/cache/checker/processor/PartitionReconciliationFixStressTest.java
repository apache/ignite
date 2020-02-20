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
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.REMOVE;

/**
 * Tests the utility under loading.
 */
public class PartitionReconciliationFixStressTest extends PartitionReconciliationStressTest {
    /**
     * Makes different variations of input params.
     */
    @Override public List<Object[]> parameters() {
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

        params.add(new Object[] {CacheAtomicityMode.ATOMIC, 1, true, LATEST, 1});
        params.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, 32, true, PRIMARY, 1});

        return params;
    }

    /**
     * Test #36 Stress test for reconciliation with -fix under load
     *
     * @throws Exception If failed.
     */
    @Override public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        super.reconciliationOfColdKeysUnderLoad(() -> assertFalse(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts()));

    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }
}
