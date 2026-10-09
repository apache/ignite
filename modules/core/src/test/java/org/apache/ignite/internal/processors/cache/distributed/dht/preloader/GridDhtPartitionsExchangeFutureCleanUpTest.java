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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies that {@link GridDhtPartitionsFullMessage} resources are released via
 * {@link GridDhtPartitionsExchangeFuture#cleanUp()} after
 * {@code GridCachePartitionExchangeManager.onExchangeDone} triggers cleanup of old futures.
 */
public class GridDhtPartitionsExchangeFutureCleanUpTest extends GridCommonAbstractTest {
    /** Number of join/leave cycles to ensure ≥ 11 exchange futures are created. */
    private static final int CYCLES = 5;

    /**
     * Triggers multiple node join/leave cycles to create enough exchange futures,
     * then verifies that the oldest futures have been cleaned up (their {@code GridDhtPartitionsFullMessage} fields are nulled).
     */
    @Test
    public void testExchangeFutureCleanUpAfterOnExchangeDone() throws Exception {
        startGrids(3);

        // Trigger 5 join/leave cycles on a 4th node.
        // Each cycle = 2 topology changes (join + leave).
        // Total: 3 (initial) + 10 (5 cycles) = 13 exchange futures.
        for (int i = 0; i < CYCLES; i++) {
            startGrid("node" + (i + 1));

            awaitPartitionMapExchange();

            stopGrid("node" + (i + 1));

            awaitPartitionMapExchange();
        }

        List<GridDhtPartitionsExchangeFuture> futs = grid(0).context().cache().context().exchange().exchangeFutures();

        assertTrue("Expected at least 11 exchange futures, got " + futs.size(), futs.size() >= 11);

        // The list is ordered newest-first. Verify that the LAST (oldest) futures have been cleaned up.
        GridDhtPartitionsExchangeFuture oldest = futs.get(futs.size() - 1);

        assertTrue("Oldest future must be done", oldest.isDone());

        verifyFutureState(oldest, true);

        // Verify that the newest future has NOT been cleaned up (it's within the protection window).
        GridDhtPartitionsExchangeFuture newest = futs.get(0);

        assertTrue("Newest future must be done", newest.isDone());

        verifyFutureState(newest, false);
    }

    /**
     * Verifies the cleanUp state of an exchange future's {@code GridDhtPartitionsFullMessage}.
     *
     * @param fut Exchange future to check.
     * @param expectCleaned {@code true} if all heavy fields must be null, {@code false} if at least one must be non-null.
     */
    private static void verifyFutureState(GridDhtPartitionsExchangeFuture fut, boolean expectCleaned) {
        Object finishState = U.field(fut, "finishState");

        if (finishState == null) {
            if (expectCleaned)
                fail("Cannot verify cleanUp state: finishState is null on a done future, expected non-null");

            return;
        }

        Object msg = U.field(finishState, "msg");

        if (msg == null) {
            if (expectCleaned)
                fail("Cannot verify cleanUp state: finishState.msg is null, expected non-null to check field cleanup");

            return;
        }

        GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage)msg;

        boolean cleaned = fullMsg.locParts == null && fullMsg.partCntrs == null && fullMsg.partitionSizes() == null;

        assertEquals("CleanUp state mismatch [locParts=" + fullMsg.locParts
            + ", partCntrs=" + fullMsg.partCntrs + ", partsSizes=" + fullMsg.partitionSizes() + "]",
            expectCleaned, cleaned);
    }
}
