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

package org.apache.ignite.internal.client.thin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.IntStream.range;

/**
 * Tests for duplication in channels' list.
 */
public class ReliableChannelDuplicationTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test after cluster restart the number of channels remains equal to the number of nodes.
     */
    @ParameterizedTest(name = "gridCount = {0}")
    @ValueSource(ints = {1, 3})
    public void testDuplicationOnClusterRestart(int gridCnt) throws Exception {
        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        stopAllGrids();

        startGrids(gridCnt);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
    }

    /**
     * Test behavior after stopping a single node in the cluster.
     */
    @Test
    public void testStopSingleNodeDuringOperation() throws Exception {
        testChannelDuplication(3, 1, 0);
    }

    /**
     * Test behavior after stopping and restarting a node.
     */
    @Test
    public void testStopAndRestartNode() throws Exception {
        testChannelDuplication(3, 1, 1);
    }

    /**
     * Test behavior after stopping multiple nodes in the cluster.
     */
    @Test
    public void testStopMultipleNodesDuringOperation() throws Exception {
        testChannelDuplication(3, 2, 2);
    }

    /**
     * Asserts that there are no duplicate channels in the list of holders based on their remote addresses.
     *
     * @param holders List of channel holders.
     */
    private void assertNoDuplicates(List<ReliableChannel.ClientChannelHolder> holders) {
        Set<InetSocketAddress> addrs = new HashSet<>();

        for (ReliableChannel.ClientChannelHolder holder : holders) {
            holder.getAddresses().forEach(addr -> {
                if (!addrs.add(addr))
                    throw new AssertionError("Duplicate remote address found: " + addr);
            });
        }
    }

    /**
     * Stop a Node and provide an operation to notify the client about new topology.
     */
    private void stopNodeAndMakeTopologyChangeDetection(int idx) {
        stopGrid(idx);

        detectTopologyChange();
    }

    /**
     * Tests that no duplicate channel holders are created during node restarts and topology changes.
     *
     * @param gridCnt Grids to start.
     * @param gridsStop int Grids to stop.
     * @param gridsRestart int Grids to restart after stop.
     */
    private void testChannelDuplication(int gridCnt, int gridsStop, int gridsRestart) throws Exception {
        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        for (int i = 0; i < gridsStop; i++) {
            stopNodeAndMakeTopologyChangeDetection(i);

            assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
        }

        for (int i = 0; i < gridsRestart; i++) {
            startGrid(i);

            detectTopologyChange();

            awaitChannelsInit(i);

            assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
        }
    }
}
