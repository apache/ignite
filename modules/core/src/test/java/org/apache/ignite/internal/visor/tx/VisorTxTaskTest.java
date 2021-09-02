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

package org.apache.ignite.internal.visor.tx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.GridTestJobContext;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.lang.IgniteUuid.randomUuid;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for VisorTxTaskTest
 */
public class VisorTxTaskTest {
    /** Tested task. */
    private VisorTxTask task;

    /** Mocked compute results. */
    private List<ComputeJobResult> results;

    /** Dummy test nodes. */
    private List<ClusterNode> nodes;

    /** Random object. */
    private static final Random r = new Random();

    /** Prepare tested task. */
    @Before
    public void beforeTest() {
        generateTxInfo(1);
    }

    /**
     * Exeteneded task enables preparing specific fields.
     */
    private static class VisorTxTestTask extends VisorTxTask {

        /**
         * Constructor with VisorTxTaskArg.
         *
         * @param arg - task arguments.
         */
        public VisorTxTestTask(VisorTxTaskArg arg) {
            taskArg = arg;
        }
    }

    /**
     * Test for reduce algorithm.
     * Every spread holistic transaction must be displayed for only near node.
     */
    @Test
    public void testGatherTxInfoToSingleBagOnReduce() {
        VisorTxTaskArg args = new VisorTxTaskArg(VisorTxOperation.INFO, 5, null, null,
            null, null, null, null, null, null, null);

        task = new VisorTxTestTask(args);

        //do reduce
        Map<ClusterNode, VisorTxTaskResult> afterReduce = task.reduce0(results);

        //Every part of the same transaction must be deleted from not near node info
        assertEquals(1, afterReduce.get(nodes.get(0)).getInfos().size());
        assertEquals(1, afterReduce.get(nodes.get(1)).getInfos().size());
        assertEquals(1, afterReduce.get(nodes.get(2)).getInfos().size());
    }

    /**
     * Test with zero limit.
     */
    @Test
    public void testReduceWithZeroLimit() {
        VisorTxTaskArg args = new VisorTxTaskArg(VisorTxOperation.INFO, 0, null, null,
            null, null, null, null, null, null, null);

        task = new VisorTxTestTask(args);

        Map<ClusterNode, VisorTxTaskResult> afterReduce = task.reduce0(results);
        assertTrue(afterReduce.isEmpty());
    }

    /**
     * Test limit parameter.
     */
    @Test
    public void testReduceLimit() {
        generateTxInfo(5);
        //prepare test case
        VisorTxTaskArg args = new VisorTxTaskArg(VisorTxOperation.INFO, 2, null, null,
            null, null, null, null, null, null, null);

        task = new VisorTxTestTask(args);

        Map<ClusterNode, VisorTxTaskResult> afterReduce = task.reduce0(results);
        assertEquals(2, afterReduce.get(nodes.get(0)).getInfos().size());
        assertEquals(2, afterReduce.get(nodes.get(1)).getInfos().size());
        assertEquals(2, afterReduce.get(nodes.get(2)).getInfos().size());
    }

    /**
     * Create random transaction info with appropriate xid and nearXid.
     *
     * @param nearXid near transaction xid.
     * @return mock VisorTxInfo.
     */
    private VisorTxInfo mockInfo(IgniteUuid xid, IgniteUuid nearXid, ClusterNode masterNodeId) {
        return new VisorTxInfo(xid,
            r.nextLong(),
            r.nextLong(),
            null,
            null,
            r.nextLong(),
            "some label",
            null,
            ACTIVE,
            0,
            nearXid,
            Collections.singleton(masterNodeId.id()),
            null,
            null);
    }

    /**
     * Create dummy job result with input infos and node.
     *
     * @param infos - infos from.
     * @param node - info provider node.
     * @return mock ComputeJobResult.
     */
    private ComputeJobResult mockJobResult(List<VisorTxInfo> infos, ClusterNode node) {
        return new GridTestJobResult(
            new VisorTxTaskResult(infos),
            null,
            null,
            node,
            new GridTestJobContext()
        );
    }

    /**
     * Generate {@link VisorTxTaskTest#results} with {@link VisorTxTaskTest#nodes}.
     * node2 (nodes[2]) is near node for one transaction only. No other near node for any transaction exists.
     *
     * @param txInfoVolume - how much tx info will be generated.
     */
    private void generateTxInfo(int txInfoVolume) {
        nodes = new ArrayList<>(3);

        ClusterNode node0 = new GridTestNode(UUID.randomUUID());
        nodes.add(node0);

        ClusterNode node1 = new GridTestNode(UUID.randomUUID());
        nodes.add(node1);

        ClusterNode node2 = new GridTestNode(UUID.randomUUID());
        nodes.add(node2);

        IgniteUuid mutualNearXid = randomUuid();

        List<VisorTxInfo> fromNode0 = new ArrayList<>();
        fromNode0.add(mockInfo(randomUuid(), mutualNearXid, node2));

        List<VisorTxInfo> fromNode1 = new ArrayList<>();
        fromNode1.add(mockInfo(randomUuid(), mutualNearXid, node2));

        List<VisorTxInfo> fromNode2 = new ArrayList<>();
        fromNode2.add(mockInfo(mutualNearXid, mutualNearXid, node2));

        for (int i = 0; i < txInfoVolume; i++) {
            fromNode0.add(mockInfo(randomUuid(), randomUuid(), node0));

            fromNode1.add(mockInfo(randomUuid(), randomUuid(), node1));

            //Every node should have same number of displayed info.
            if (i > 0)
                fromNode2.add(mockInfo(randomUuid(), randomUuid(), node2));

        }

        results = new ArrayList<>(3);
        results.add(mockJobResult(fromNode0, node0));
        results.add(mockJobResult(fromNode1, node1));
        results.add(mockJobResult(fromNode2, node2));
    }

}
