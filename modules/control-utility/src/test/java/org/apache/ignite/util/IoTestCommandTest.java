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
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCommunication() throws Exception {
        IgniteEx srv = startGrids(2);

        executeCommand(
            EXIT_CODE_OK,
            "--io-test", "communication",
            NODE_ID, srv.localNode().id().toString(),
            "--warmup", "0",
            "--duration", "100",
            "--threads", "1",
            "--payload-size", "32"
        );

        String output = String.valueOf(lastOperationResult);

        assertTrue(output, output.contains("Communication SPI test"));
        assertTrue(output, output.contains("Source node: " + srv.localNode().id()));
        assertTrue(output, output.contains("Payload: 32 bytes each way"));
        assertTrue(output, output.contains("Message handling: system pool"));
        assertTrue(output, output.contains("Target node: " + grid(1).localNode().id()));
        assertFalse(output, output.contains("Target node: " + srv.localNode().id()));
        assertTrue(output, output.contains("End-to-end RTT (ms):"));
        assertTrue(output, output.contains("Node-local delays (ms, min/avg/max):"));
        assertTrue(output, output.contains("Source request pre-serialization delay:"));
        assertTrue(output, output.contains("Target request dispatch delay:"));
        assertTrue(output, output.contains("Estimated one-way message delay (ms, min/avg/max):"));
        assertTrue(output, output.contains("Clock assumption: synchronized wall clocks"));
        assertTrue(output, output.contains("Request (serialization start -> deserialization complete):"));
        assertTrue(output, output.contains("Response (serialization start -> deserialization complete):"));
    }

    /** */
    @Test
    public void testDiscovery() throws Exception {
        startGrids(3);

        executeCommand(
            EXIT_CODE_OK,
            "--io-test", "discovery",
            "--samples", "3",
            "--interval", "10",
            "--payload-size", "32"
        );

        String output = String.valueOf(lastOperationResult);

        assertTrue(output, output.contains("TcpDiscoverySpi ring test"));
        assertTrue(output, output.contains("Coordinator: " + grid(0).localNode().id()));
        assertTrue(output, output.contains("Samples: 3 | Inter-sample delay: 10 ms"));
        assertTrue(output, output.contains("Request application payload: 32 bytes"));
        assertTrue(output, output.contains("Server ring path:"));
        assertTrue(output, output.contains(grid(0).localNode().id().toString()));
        assertTrue(output, output.contains(grid(1).localNode().id().toString()));
        assertTrue(output, output.contains(grid(2).localNode().id().toString()));
        assertTrue(output, output.contains("Request ring latency (submission -> local ACK, ms):"));
        assertTrue(output, output.contains("Estimated per-hop one-way message delay (ms, min/avg/max):"));
        assertTrue(output, output.contains("Clock assumption: synchronized wall clocks"));
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
