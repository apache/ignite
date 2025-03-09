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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.IntStream.range;

/**
 * Tests for duplication in channels' list.
 */
@RunWith(Parameterized.class)
public class ReliableChannelDuplicationTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** Grid count. */
    @Parameterized.Parameter(0)
    public int gridCnt;

    @Parameterized.Parameters(name = "gridCount = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { 1 },
            { 3 }
        });
    }

    /**
     * Asserts that there are no duplicate channels in the list of holders based on their remote addresses.
     *
     * @param holders List of channel holders.
     */
    private void assertNoDuplicates(List<ReliableChannel.ClientChannelHolder> holders) {
        Set<String> addresses = new TreeSet<>();

        for (ReliableChannel.ClientChannelHolder holder : holders) {
            String addr = holder.getAddresses().toString();

            if (!addresses.add(addr))
                throw new AssertionError("Duplicate remote address found: " + addr);
        }
    }

    /**
     * Stop a Node and provide an operation to notify the client about new topology.
     */
    private void stopNodeAndMakeTopologyChangeDetection(int idx) {
        stopGrid(idx);

        detectTopologyChange();

        // Address of stopped node removed.
        channels[idx] = null;

        client.cacheNames();
    }

    /**
     * Test after cluster restart the number of channels remains equal to the number of nodes.
     */
    @Test
    public void testDuplicationOnClusterRestart() throws Exception {
        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        stopAllGrids();

        startGrids(gridCnt);

        client.cacheNames();

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
    }

    /**
     * Test behavior after stopping a single node in the cluster.
     */
    @Test
    public void testStopSingleNodeDuringOperation() throws Exception {
        Assume.assumeFalse(gridCnt == 1);

        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        // Stop one node.
        stopNodeAndMakeTopologyChangeDetection(0);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
    }

    /**
     * Test behavior after stopping and restarting a node.
     */
    @Test
    public void testStopAndRestartNode() throws Exception {
        Assume.assumeFalse(gridCnt == 1);

        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        // Stop one node.
        stopNodeAndMakeTopologyChangeDetection(0);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        // Restart the stopped node.
        startGrid(0);

        detectTopologyChange();

        awaitChannelsInit(0);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
    }

    /**
     * Test behavior after stopping multiple nodes in the cluster.
     */
    @Test
    public void testStopMultipleNodesDuringOperation() throws Exception {
        Assume.assumeFalse(gridCnt < 3);

        startGrids(gridCnt);

        initClient(getClientConfiguration(range(0, gridCnt).toArray()), range(0, gridCnt).toArray());

        // Stop two nodes.
        stopNodeAndMakeTopologyChangeDetection(0);
        stopNodeAndMakeTopologyChangeDetection(1);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());

        // Restart the stopped nodes.
        startGrid(0);
        startGrid(1);

        detectTopologyChange();

        awaitChannelsInit(0);

        assertNoDuplicates(((TcpIgniteClient)client).reliableChannel().getChannelHolders());
    }
}