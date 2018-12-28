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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for join partition pruning.
 */
@RunWith(JUnit4.class)
public class JoinPartitionPruningSelfTest extends GridCommonAbstractTest {
    /** Number of intercepted requests. */
    private static final AtomicInteger INTERCEPTED_REQS = new AtomicInteger();

    /** Parititions tracked during query execution. */
    private static final ConcurrentSkipListSet<Integer> INTERCEPTED_PARTS = new ConcurrentSkipListSet<>();

    /**
     * Clear partitions.
     */
    private static void clearIoState() {
        INTERCEPTED_REQS.set(0);
        INTERCEPTED_PARTS.clear();
    }

    /**
     * Make sure that expected partitions are logged.
     *
     * @param expParts Expected partitions.
     */
    private static void assertPartitions(int... expParts) {
        Collection<Integer> expParts0 = new TreeSet<>();

        for (int expPart : expParts)
            expParts0.add(expPart);

        assertPartitions(expParts0);
    }

    /**
     * Make sure that expected partitions are logged.
     *
     * @param expParts Expected partitions.
     */
    private static void assertPartitions(Collection<Integer> expParts) {
        TreeSet<Integer> expParts0 = new TreeSet<>(expParts);
        TreeSet<Integer> actualParts = new TreeSet<>(INTERCEPTED_PARTS);

        assertEquals("Unexpected partitions [exp=" + expParts + ", actual=" + actualParts + ']',
            expParts0, actualParts);
    }

    /**
     * Make sure that no partitions were extracted.
     */
    private static void assertNoPartitions() {
        assertTrue("No requests were sent.", INTERCEPTED_REQS.get() > 0);
        assertTrue("Partitions are not empty: " + INTERCEPTED_PARTS, INTERCEPTED_PARTS.isEmpty());
    }

    /**
     * Make sure there were no requests sent because we determined empty partition set.
     */
    private static void assertNoRequests() {
        assertEquals("Requests were sent: " + INTERCEPTED_REQS.get(), 0, INTERCEPTED_REQS.get());
    }

    /**
     * TCP communication SPI which will track outgoing query requests.
     */
    private static class TrackingTcpCommunicationSpi extends TcpCommunicationSpi {
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridH2QueryRequest) {
                    INTERCEPTED_REQS.incrementAndGet();

                    GridH2QueryRequest req = (GridH2QueryRequest)msg0.message();

                    int[] parts = req.queryPartitions();

                    if (!F.isEmpty(parts)) {
                        for (int part : parts)
                            INTERCEPTED_PARTS.add(part);
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
