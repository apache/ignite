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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.client.message.GridClientPingPacket;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeTrue;

/** */
public class GridCommandHandlerSslReloadTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    @Rule
    public final TemporaryFolder stores = new TemporaryFolder();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        persistenceEnable(false);
        injectTestSystemOut();
    }

    /** */
    @Test
    public void testSharedFactoryReloadedOnceOnServerAndClient() throws Exception {
        List<IgniteEx> nodes = new ArrayList<>();
        List<CountingSslContextFactory> factories = new ArrayList<>();

        for (String name : new String[] {getTestIgniteInstanceName(0), "client"}) {
            CountingSslContextFactory factory = new CountingSslContextFactory(keyStorePath("node01"));
            IgniteConfiguration cfg = getConfiguration(name).setSslContextFactory(factory);

            cfg.getClientConnectorConfiguration().setSslEnabled(true);
            cfg.getConnectorConfiguration().setSslEnabled(true);

            nodes.add(startGrid(cfg));
            factories.add(factory);
        }

        int[] loads = factories.stream().mapToInt(factory -> factory.loads.get()).toArray();

        assertEquals(EXIT_CODE_OK, executeTlsReload());

        for (int i = 0; i < nodes.size(); i++) {
            assertEquals(loads[i] + 1, factories.get(i).loads.get());
            String transports = nodes.get(i).configuration().isClientMode()
                ? "communication, client connector, discovery"
                : "communication, client connector, binary REST, discovery";

            assertContains(log, testOut.toString(), nodes.get(i).localNode().id() + ": " + transports + ": reloaded");
        }
    }

    /** */
    @Test
    public void testReloadedFilesApplyToNewConnections() throws Exception {
        Path keys = stores.newFile("keys.jks").toPath();
        Path trust = stores.newFile("trust.jks").toPath();

        Files.copy(Path.of(keyStorePath("node01")), keys, REPLACE_EXISTING);
        Files.copy(Path.of(keyStorePath("trustone")), trust, REPLACE_EXISTING);

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(keyStorePassword().toCharArray());
        factory.setTrustStoreFilePath(trust.toString());
        factory.setTrustStorePassword(keyStorePassword().toCharArray());

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        cfg.getConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true).setSslFactory(factory);

        IgniteEx node = startGrid(cfg);
        int port = node.localNode().attribute(ATTR_REST_TCP_PORT);

        connectAndPing(port, sslTrustedFactory("node01", "trustone").create());

        Files.copy(Path.of(keyStorePath("node02")), keys, REPLACE_EXISTING);
        Files.copy(Path.of(keyStorePath("trusttwo")), trust, REPLACE_EXISTING);

        assertEquals(EXIT_CODE_OK, execute("--ssl", "reload"));

        connectAndPing(port, sslTrustedFactory("node02", "trusttwo").create());
    }

    /** */
    @Test
    public void testPartialFailureReportsEveryFactory() throws Exception {
        Path keys = stores.newFile("keys.jks").toPath();
        LogListener loadFailure = LogListener.matches(EOFException.class.getName()).build();

        Files.copy(Path.of(keyStorePath("node01")), keys, REPLACE_EXISTING);

        CountingSslContextFactory failing = new CountingSslContextFactory(keys.toString());
        CountingSslContextFactory working = new CountingSslContextFactory(keyStorePath("node01"));
        SSLContext clientCtx = working.create();
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0)).setSslContextFactory(failing);

        cfg.setGridLogger(new ListeningTestLogger(log, loadFailure));
        cfg.getClientConnectorConfiguration().setSslEnabled(true).setUseIgniteSslContextFactory(false).setSslContextFactory(working);
        cfg.getConnectorConfiguration().setSslEnabled(true).setSslFactory(() -> clientCtx);

        IgniteEx node = startGrid(cfg);

        Files.write(keys, new byte[0]);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeTlsReload());

        String report = testOut.toString();
        String nodeId = node.localNode().id().toString();

        assertContains(log, report, nodeId + ": communication, discovery: failed:");
        assertContains(log, report, nodeId + ": client connector: reloaded");
        assertContains(log, report, nodeId + ": binary REST: unsupported:");
        assertTrue(loadFailure.check());
        assertNotSame(clientCtx, working.create());
    }

    /** */
    @Test
    public void testDoesNotReloadRestFactoryWhenListenerFailedToStart() throws Exception {
        CountingSslContextFactory factory = new CountingSslContextFactory(keyStorePath("node01"));

        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress("127.0.0.1", 0));

            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

            cfg.getConnectorConfiguration().setPort(occupied.getLocalPort()).setPortRange(0).setSslEnabled(true).setSslFactory(factory);

            IgniteEx node = startGrid(cfg);
            int loads = factory.loads.get();

            assertEquals(EXIT_CODE_OK, execute("--ssl", "reload"));
            assertEquals(loads, factory.loads.get());
            assertContains(log, testOut.toString(), node.localNode().id() + ": no TCP transports started with TLS");
        }
    }

    /** */
    @Test
    public void testDeclinedConfirmationDoesNotReload() throws Exception {
        assumeTrue(cliCommandHandler());

        CountingSslContextFactory factory = new CountingSslContextFactory(keyStorePath("node01"));

        startGrid(getConfiguration(getTestIgniteInstanceName(0)).setSslContextFactory(factory));

        int loads = factory.loads.get();

        autoConfirmation = false;
        injectTestSystemIn("n");

        assertEquals(EXIT_CODE_OK, execute("--ssl", "reload"));
        assertContains(log, testOut.toString(),
            "Make sure that all nodes and client applications trust the new certificates before reloading.");
        assertEquals(loads, factory.loads.get());
    }

    /** */
    private int executeTlsReload() {
        return execute("--ssl", "reload", "--keystore", keyStorePath("node01"), "--keystore-password", keyStorePassword(),
            "--truststore", keyStorePath("trustone"), "--truststore-password", keyStorePassword());
    }

    /** */
    private static void connectAndPing(int port, SSLContext ctx) throws Exception {
        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress("127.0.0.1", port), 10_000);
            sock.setSoTimeout(10_000);
            sock.startHandshake();
            sock.getOutputStream().write(GridClientPingPacket.PING_PACKET);

            assertArrayEquals(GridClientPingPacket.PING_PACKET,
                sock.getInputStream().readNBytes(GridClientPingPacket.PING_PACKET.length));
        }
    }

    /** */
    private static class CountingSslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private final AtomicInteger loads = new AtomicInteger();

        /** */
        private CountingSslContextFactory(String path) {
            setKeyStoreFilePath(path);
            setKeyStorePassword(keyStorePassword().toCharArray());
            setTrustManagers(getDisabledTrustManager());
        }

        /** {@inheritDoc} */
        @Override protected InputStream openFileInputStream(String path) throws IOException {
            loads.incrementAndGet();

            return super.openFileInputStream(path);
        }
    }
}
