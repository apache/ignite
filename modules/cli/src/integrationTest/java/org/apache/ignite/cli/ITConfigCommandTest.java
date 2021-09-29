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
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@code ignite config} commands.
 */
public class ITConfigCommandTest extends AbstractCliTest {
    /** DI context. */
    private ApplicationContext ctx;

    /** stderr. */
    private ByteArrayOutputStream err;

    /** stdout. */
    private ByteArrayOutputStream out;

    /** Port for REST communication */
    private int restPort;

    /** Port for thin client communication */
    private int clientPort;

    /** Network port. */
    private int networkPort;

    /** */
    @BeforeEach
    void setup(@TempDir Path workDir, TestInfo testInfo) throws IOException {
        // TODO: IGNITE-15131 Must be replaced by receiving the actual port configs from the started node.
        // This approach still can produce the port, which will be unavailable at the moment of node start.
        restPort = getAvailablePort();
        clientPort = getAvailablePort();
        networkPort = getAvailablePort();

        String configStr = "network.port=" + networkPort + "\n" +
            "rest.port=" + restPort + "\n" + "rest.portRange=0" + "\n" +
            "clientConnector.port=" + clientPort + "\n" + "clientConnector.portRange=0";

        IgnitionManager.start(testNodeName(testInfo, networkPort), configStr, workDir);

        ctx = ApplicationContext.run(Environment.TEST);

        err = new ByteArrayOutputStream();
        out = new ByteArrayOutputStream();
    }

    /** */
    @AfterEach
    void tearDown(TestInfo testInfo) {
        IgnitionManager.stop(testNodeName(testInfo, networkPort));
        ctx.stop();
    }

    /**
     * Creates a unique Ignite node name for the given test.
     */
    private static String testNodeName(TestInfo testInfo, int port) {
        return testInfo.getTestClass()
            .map(Class::getCanonicalName)
            .flatMap(clsName -> testInfo.getTestMethod().map(method -> clsName + '#' + method.getName()))
            .map(name -> name + ':' + port)
            .orElseThrow();
    }

    @Test
    public void setAndGetWithManualHost() {
        int exitCode = cmd(ctx).execute(
            "config",
            "set",
            "--node-endpoint",
            "localhost:" + restPort,
            "--type", "node", //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
            "node.metastorageNodes=[\"localhost1\"]"
        );

        String nl = System.lineSeparator();

        assertEquals(0, exitCode);
        assertEquals(
            "Configuration was updated successfully." + nl + nl +
                "Use the ignite config get command to view the updated configuration." + nl,
            out.toString()
        );

        resetStreams();

        exitCode = cmd(ctx).execute(
            "config",
            "get",
            "--node-endpoint",
            "localhost:" + restPort,
            "--type", "node" //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        );

        assertEquals(0, exitCode);
        assertEquals(
            "\"{\"clientConnector\":{\"connectTimeout\":5000,\"port\":" + clientPort + ",\"portRange\":0}," +
                "\"network\":{\"netClusterNodes\":[],\"port\":" + networkPort + "}," +
                "\"node\":{\"metastorageNodes\":[\"localhost1\"]}," +
                "\"rest\":{\"port\":" + restPort + ",\"portRange\":0}}\"" + nl,
            unescapeQuotes(out.toString())
        );
    }

    @Test
    public void partialGet() {
        int exitCode = cmd(ctx).execute(
            "config",
            "get",
            "--node-endpoint",
            "localhost:" + restPort,
            "--selector",
            "network",
            "--type", "node" //TODO: Fix in https://issues.apache.org/jira/browse/IGNITE-15306
        );

        assertEquals(0, exitCode);
        assertEquals(
            "\"{\"netClusterNodes\":[],\"port\":" + networkPort + "}\"" + System.lineSeparator(),
            unescapeQuotes(out.toString())
        );
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

    private String unescapeQuotes(String input) {
        return input.replace("\\\"", "\"");
    }
}
