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

import org.junit.Test;

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
     * Test after single node cluster restart remains single channel.
     */
    @Test
    public void testSingleNodeDuplicationOnClusterRestart() throws Exception {
        startGrids(1);
        initClient(getClientConfiguration(range(0, 1).toArray()), range(0, 1).toArray());

        assertEquals(1, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());

        stopAllGrids();

        startGrids(1);

        assertEquals(1, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());
    }

    /**
     * Test after cluster restart the number of channels remains equal to the number of nodes.
     */
    @Test
    public void testMultiplyNodeDuplicationOnClusterRestart() throws Exception {
        startGrids(3);
        initClient(getClientConfiguration(range(0, 3).toArray()), range(0, 3).toArray());

        assertEquals(3, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());

        stopAllGrids();

        startGrids(3);

        assertEquals(3, ((TcpIgniteClient)client).reliableChannel().getChannelHolders().size());
    }
}
