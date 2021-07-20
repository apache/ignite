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

package org.apache.ignite.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.nio.file.Path;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@code ignite config} commands.
 */
public class ConfigCommandTest extends AbstractCliTest {
    /** DI context. */
    private ApplicationContext ctx;

    /** stderr. */
    private ByteArrayOutputStream err;

    /** stdout. */
    private ByteArrayOutputStream out;

    /** Port for REST communication */
    private int restPort;

    /** Network port. */
    private int networkPort;

    /** Ignite node. */
    private Ignite node;

    /** */
    @BeforeEach
    private void setup(@TempDir Path workDir) throws IOException {
        // TODO: IGNITE-15131 Must be replaced by receiving the actual port configs from the started node.
        // This approach still can produce the port, which will be unavailable at the moment of node start.
        restPort = getAvailablePort();
        networkPort = getAvailablePort();

        String cfgStr = "network.port=" + networkPort + "\n" +
            "rest.port=" + restPort;

        node = IgnitionManager.start("node1", cfgStr, workDir);

        ctx = ApplicationContext.run(Environment.TEST);

        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
    }

    /**
     * Cleans environment after each test.
     *
     * @throws Exception If failed to close ignite node or application context.
     */
    // TODO: IGNITE-14581 Node must be stopped here.
    @AfterEach
    private void tearDown() throws Exception {
        node.close();

        ctx.stop();
    }

    /**
     * Tests 'config set' and 'config get' commands.
     */
    @Test
    public void setAndGetWithManualHost() {
        int exitCode = cmd(ctx).execute(
            "config",
            "set",
            "--node-endpoint",
            "localhost:" + restPort,
            "node.metastorageNodes=[\"localhost1\"]");

        assertEquals(0, exitCode, "The command 'config set' failed [code=" + exitCode + ']');
        assertEquals(
            unescapeQuotes("Configuration was updated successfully.\n" + "\n" +
                "Use the ignite config get command to view the updated configuration.\n"),
            unescapeQuotes(out.toString()),
            "The command 'config set' was successfully completed, " +
                "but the server response does not match with expected.");

        resetStreams();

        exitCode = cmd(ctx).execute(
            "config",
            "get",
            "--node-endpoint",
            "localhost:" + restPort);

        assertEquals(0, exitCode, "The command 'config get' failed [exitCode=" + exitCode + ']');
        assertEquals(
            "\"{\"network\":{\"port\":" + networkPort + ",\"netClusterNodes\":[]}," +
                "\"node\":{\"metastorageNodes\":[\"localhost1\"]}," +
                "\"rest\":{\"port\":" + restPort + ",\"portRange\":0}}\"",
            unescapeQuotes(out.toString()),
            "The command 'config get' was successfully completed, " +
                "but the server response does not match with expected.");
    }

    /**
     * Tests partial 'config get' command.
     */
    @Test
    public void partialGet() {
        int exitCode = cmd(ctx).execute(
            "config",
            "get",
            "--node-endpoint",
            "localhost:" + restPort,
            "--selector",
            "network");

        assertEquals(0, exitCode, "The command 'config get' failed [exitCode=" + exitCode + ']');
        assertEquals(
            "\"{\"port\":"+ networkPort + ",\"netClusterNodes\":[]}\"",
            unescapeQuotes(out.toString()),
            "The command 'config get' was successfully completed, " +
                "but the server response does not match with expected.");
    }

    /**
     * @return Any available port.
     * @throws IOException if can't allocate port to open socket.
     */
    // TODO: Must be removed after IGNITE-15131.
    private int getAvailablePort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        s.close();
        return s.getLocalPort();
    }

    /**
     * @param applicationCtx DI context.
     * @return New command line instance.
     */
    private CommandLine cmd(ApplicationContext applicationCtx) {
        CommandLine.IFactory factory = new CommandFactory(applicationCtx);

        return new CommandLine(IgniteCliSpec.class, factory)
            .setErr(new PrintWriter(err, true))
            .setOut(new PrintWriter(out, true));
    }

    /**
     * Reset stderr and stdout streams.
     */
    private void resetStreams() {
        err.reset();
        out.reset();
    }

    /**
     * Removes unescaped quotes and new line symbols.
     *
     * @param input Input string.
     * @return New string without new lines and unescaped quotes.
     */
    private String unescapeQuotes(String input) {
        return input
            .replace("\\\"", "\"")
            .replaceAll("\\r", "")
            .replaceAll("\\n", "");
    }
}
