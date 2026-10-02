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

package org.apache.ignite.util;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.io.IoTestCommand;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.SystemViewCommandTest.NODE_ID;

/**
 * Tests for the {@link IoTestCommand}.
 */
public class IoTestCommandTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCommunication() throws Exception {
        IgniteEx srv = startGrids(2);

        String output = executeCommand(
            EXIT_CODE_OK,
            "--io-test", "communication",
            NODE_ID, srv.localNode().id().toString(),
            "--warmup", "0",
            "--duration", "100",
            "--threads", "1",
            "--payload-size", "32",
            "--process-in-nio-thread"
        );

        assertTrue(output, output.contains("Communication SPI test"));
        assertTrue(output, output.contains("id=" + srv.localNode().id()));
        assertTrue(output, output.contains("Parameters: warmup=0 ms | duration=100 ms | threads=1 | payload=32 bytes"));
        assertTrue(output, output.contains("Handling: NIO thread"));
        assertTrue(output, output.contains(grid(1).localNode().consistentId().toString()));
        assertFalse(output, output.contains(grid(1).localNode().id().toString()));
        assertTrue(output, output.contains("RTT:"));
        assertTrue(output, output.contains("Target"));
        assertTrue(output, output.contains("Samples"));
        assertTrue(output, output.contains("Min, ms"));
        assertTrue(output, output.contains("Avg, ms"));
        assertTrue(output, output.contains("Max, ms"));
        assertTrue(output, output.contains("Estimated one-way delivery*:"));
        assertTrue(output, output.contains("Request"));
        assertTrue(output, output.contains("Response"));
        assertTrue(output, output.contains("OS wall-clock time and requires synchronized node clocks"));
        assertTrue(output, output.contains("RTT uses a monotonic clock"));
        assertFalse(output, output.contains("System-pool handling includes executor dispatch time"));
        assertFalse(output, output.contains("RTT histogram"));
    }

    /** */
    @Test
    public void testDiscovery() throws Exception {
        startGrids(3);

        String output = executeCommand(
            EXIT_CODE_OK,
            "--io-test", "discovery",
            "--samples", "3",
            "--interval", "10",
            "--payload-size", "32"
        );

        assertTrue(output, output.contains("TcpDiscoverySpi ring test"));
        assertTrue(output, output.contains("id=" + grid(0).localNode().id()));
        assertTrue(output, output.contains("Parameters: samples=3 | interval=10 ms | payload=32 bytes"));
        assertFalse(output, output.contains("Server ring path:"));
        assertTrue(output, output.contains(grid(0).localNode().consistentId().toString()));
        assertTrue(output, output.contains(grid(1).localNode().consistentId().toString()));
        assertTrue(output, output.contains(grid(2).localNode().consistentId().toString()));
        assertFalse(output, output.contains(grid(1).localNode().id().toString()));
        assertFalse(output, output.contains(grid(2).localNode().id().toString()));
        assertTrue(output, output.contains("Full-ring latency:"));
        assertTrue(output, output.contains("Samples"));
        assertTrue(output, output.contains("Min, ms"));
        assertTrue(output, output.contains("Avg, ms"));
        assertTrue(output, output.contains("Max, ms"));
        assertFalse(output, output.contains("P50"));
        assertFalse(output, output.contains("P95"));
        assertTrue(output, output.contains("Estimated per-hop delivery (ring order)*:"));
        assertTrue(output, output.contains("OS wall-clock time and requires synchronized node clocks"));
        assertTrue(output, output.contains("Full-ring latency uses a monotonic clock"));
    }

    /** */
    @Test
    public void testInvalidPayloadSize() {
        executeCommand(
            EXIT_CODE_INVALID_ARGUMENTS,
            "--io-test", "discovery",
            "--payload-size", "65537"
        );
    }
}
