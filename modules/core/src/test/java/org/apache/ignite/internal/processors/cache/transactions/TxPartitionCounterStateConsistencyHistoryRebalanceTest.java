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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Test partitions consistency in various scenarios.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class TxPartitionCounterStateConsistencyHistoryRebalanceTest extends TxPartitionCounterStateConsistencyTest {
    /** */
    @Test
    public void testConsistencyAfterBaselineNodeStopAndRemoval() throws Exception {
        doTestConsistencyAfterBaselineNodeStopAndRemoval(0);
    }

    /** */
    @Test
    public void testConsistencyAfterBaselineNodeStopAndRemoval_WithRestart() throws Exception {
        doTestConsistencyAfterBaselineNodeStopAndRemoval(1);
    }

    /** */
    @Test
    public void testConsistencyAfterBaselineNodeStopAndRemoval_WithRestartAndSkipCheckpoint() throws Exception {
        doTestConsistencyAfterBaselineNodeStopAndRemoval(2);
    }

    /**
     * Test a scenario when partition is evicted and owned again with non-zero initial and current counters.
     * Such partition should not be historically rebalanced, otherwise only subset of data will be rebalanced.
     */
    private void doTestConsistencyAfterBaselineNodeStopAndRemoval(int mode) throws Exception {
        backups = 2;

        final int srvNodes = SERVER_NODES + 1;

        IgniteEx prim = startGrids(srvNodes);

        prim.cluster().active(true);

        for (int p = 0; p < partitions(); p++) {
            prim.cache(DEFAULT_CACHE_NAME).put(p, p);
            prim.cache(DEFAULT_CACHE_NAME).put(p + PARTS_CNT, p * 2);
        }

        forceCheckpoint();

        stopGrid(1); // topVer=5,0

        awaitPartitionMapExchange();

        resetBaselineTopology(); // topVer=5,1

        awaitPartitionMapExchange();

        forceCheckpoint(); // Will force GridCacheDataStore.exists=true mode after part store re-creation.

        startGrid(1); // topVer=6,0

        awaitPartitionMapExchange();

        resetBaselineTopology(); // topVer=6,1

        awaitPartitionMapExchange(true, true, null);

        // Create counter difference with evicted partition so it's applicable for historical rebalancing.
        for (int p = 0; p < partitions(); p++)
            prim.cache(DEFAULT_CACHE_NAME).put(p + PARTS_CNT, p * 2 + 1);

        stopGrid(1); // topVer=7,0

        if (mode > 0) {
            stopGrid(mode == 1, grid(2).name());
            stopGrid(mode == 1, grid(3).name());

            startGrid(2);
            startGrid(3);
        }

        prim.context().cache().context().exchange().rebalanceDelay(500);

        Random r = new Random();

        AtomicBoolean stop = new AtomicBoolean();

        final IgniteInternalFuture<?> fut = doRandomUpdates(r,
            prim,
            IntStream.range(0, 1000).boxed().collect(toList()),
            prim.cache(DEFAULT_CACHE_NAME),
            stop::get);

        resetBaselineTopology(); // topVer=7,1

        awaitPartitionMapExchange();

        stop.set(true);
        fut.get();

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));
    }
}
