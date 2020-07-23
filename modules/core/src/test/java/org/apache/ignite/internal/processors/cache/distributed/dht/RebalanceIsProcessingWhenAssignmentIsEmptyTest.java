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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that a rebalance future completed when all partitions are rebalanced.
 */
public class RebalanceIsProcessingWhenAssignmentIsEmptyTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1)
                .setAffinity(new TestAffinity(getTestIgniteInstanceName(0),
                    getTestIgniteInstanceName(1),
                    getTestIgniteInstanceName(2),
                    getTestIgniteInstanceName(3))));
    }

    /**
     * Test with specific affinity on a default cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cachex(DEFAULT_CACHE_NAME).put(0, 0);
        ignite0.cachex(DEFAULT_CACHE_NAME).put(1, 0);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi.spi(ignite0).blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage supplyMsg = (GridDhtPartitionSupplyMessage)msg;

                return supplyMsg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME) &&
                    node.consistentId().equals(getTestIgniteInstanceName(1));
            }

            return false;
        });

        startGrid(3);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        stopGrid(0);

        awaitPartitionMapExchange();

        IgniteInternalFuture fut = grid(1).context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).preloader().rebalanceFuture();

        assertTrue("Rebalance completed but this rebalance future is not complete, fut=" + fut, fut.isDone());
    }

    /**
     * Tets affinity function.
     * It gives same assignment on second node in topology of three nodes and differences in fours.
     */
    public static class TestAffinity extends RendezvousAffinityFunction {
        /** Nodes consistence ids. */
        String[] nodeConsistentIds;

        /**
         * @param nodes Nodes consistence ids.
         */
        public TestAffinity(String... nodes) {
            super(false,2);

            this.nodeConsistentIds = nodes;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            int nodes = affCtx.currentTopologySnapshot().size();

            if (nodes != 4 && nodes != 3)
                return super.assignPartitions(affCtx);

            List<List<ClusterNode>> assignment = new ArrayList<>();

            assignment.add(new ArrayList<>(2));
            assignment.add(new ArrayList<>(2));

            assignment.get(1).add(null);
            assignment.get(1).add(null);
            assignment.get(0).add(null);
            assignment.get(0).add(null);

            if (nodes == 3) {
                for (ClusterNode node : affCtx.currentTopologySnapshot())
                    if (nodeConsistentIds[0].equals(node.consistentId()) ||
                        nodeConsistentIds[3].equals(node.consistentId())) {
                        assignment.get(0).set(0, node);
                        assignment.get(1).set(1, node);
                    }
                    else if (nodeConsistentIds[1].equals(node.consistentId()))
                        assignment.get(1).set(0, node);
                    else if (nodeConsistentIds[2].equals(node.consistentId()))
                        assignment.get(0).set(1, node);
                    else
                        throw new AssertionError("Unexpected node consistent id is " + node.consistentId());
            }
            else {
                for (ClusterNode node : affCtx.currentTopologySnapshot()) {
                    if (nodeConsistentIds[0].equals(node.consistentId()))
                        assignment.get(0).set(1, node);
                    else if (nodeConsistentIds[1].equals(node.consistentId())) {
                        assignment.get(1).set(0, node);
                        assignment.get(0).set(0, node);
                    }
                    else if (nodeConsistentIds[2].equals(node.consistentId()))
                        assignment.get(1).set(1, node);
                    else if (!nodeConsistentIds[3].equals(node.consistentId()))
                        throw new AssertionError("Unexpected node consistent id is " + node.consistentId());
                }
            }

            return assignment;
        }
    }
}
