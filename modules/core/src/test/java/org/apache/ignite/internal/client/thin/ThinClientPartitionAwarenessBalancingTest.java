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

import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

/**
 * Test requests to connections distribution by thin client.
 */
public class ThinClientPartitionAwarenessBalancingTest extends ThinClientAbstractPartitionAwarenessTest {
    /** */
    @Test
    public void testConnectionDistribution() throws Exception {
        startGrids(3);

        initClient(getClientConfiguration(0, 1, 2, 3), 0, 1, 2);

        BitSet usedConnections = new BitSet();

        for (int i = 0; i < 100; i++)
            client.cacheNames(); // Non-affinity requests should be randomly distributed among connections.

        List<TestTcpClientChannel> channelList = F.asList(channels);

        while (!opsQueue.isEmpty()) {
            T2<TestTcpClientChannel, ClientOperation> op = opsQueue.poll();

            usedConnections.set(channelList.indexOf(op.get1()));
        }

        assertEquals(BitSet.valueOf(new byte[] {7}), usedConnections); // 7 = set of {0, 1, 2}
    }
}
