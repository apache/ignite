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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommand;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.ssl.SslTestUtils.node;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.TestCertificateAuthority.PWD;
import static org.apache.ignite.testframework.TestCertificateAuthority.save;

/**
 * {@code --ssl reload} and {@code --ssl status} through control.sh. Only the CLI handler runs the test: it connects to a node over
 * the client connector TLS that the command reloads, while the JMX handler calls the node in process and would only repeat
 * SslContextReloadTest.
 */
public class GridCommandHandlerSslReloadTest extends GridCommandHandlerAbstractTest {
    /** Key store the nodes run on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** CLI credentials, kept readable when the node key store is replaced or corrupted. */
    private Path clientKeyStore;

    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(PWD);
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /** {@inheritDoc} */
    @Override protected void extendSslParams(List<String> params) {
        params.addAll(List.of("--keystore", clientKeyStore.toString(), "--keystore-password", new String(PWD)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(cliCommandHandler());

        keyStore = Files.createTempFile("ignite-cli-ssl-reload-", ".jks");
        clientKeyStore = Files.createTempFile("ignite-cli-ssl-client-", ".jks");

        super.beforeTest();

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        if (keyStore != null)
            Files.deleteIfExists(keyStore);

        if (clientKeyStore != null)
            Files.deleteIfExists(clientKeyStore);
    }

    /**
     * The reload asks for confirmation. One command reloads every node, client ones included, and status shows the new
     * certificate and the time of that reload; a broken store fails the command.
     */
    @Test
    public void testReloadAndStatus() throws Exception {
        assertNotNull("The reload must ask for confirmation", new SslReloadCommand().confirmationPrompt(new NoArg()));

        TestCertificateAuthority ca = new TestCertificateAuthority("cli-ca");
        String oldAlias = "initial-key";
        String replacement = "replacement-key";

        save(ca.keyStoreWithPrivateKey(oldAlias), keyStore);
        save(ca.keyStoreWithPrivateKey(), clientKeyStore);

        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);
        IgniteEx cli = startClientGrid(CLIENT_NODE_NAME_PREFIX);

        // Transports are listed alphabetically; a client node runs no binary REST.
        Map<IgniteEx, String> transports = Map.of(
            srv0, "binary REST, client connector, communication, discovery",
            srv1, "binary REST, client connector, communication, discovery",
            cli, "client connector, communication, discovery");

        String loaded = "loaded alias=" + replacement + ", subject=CN=" + replacement;

        save(ca.keyStoreWithPrivateKey(replacement), keyStore);

        long start = System.currentTimeMillis();
        String reloadOutput = executeCommand(EXIT_CODE_OK, "--ssl", "reload");

        transports.forEach((g, t) -> assertContains(log, reloadOutput, node(g) + ": reloaded " + t + "; " + loaded));

        String statusOutput = executeCommand(EXIT_CODE_OK, "--ssl", "status");

        transports.forEach((g, t) -> assertContains(log, statusOutput, node(g) + ": " + t + '\n'));

        assertContains(log, statusOutput, loaded);
        assertNotContains(log, statusOutput, "subject=CN=" + oldAlias);

        List<Instant> succeeded = Pattern.compile("last reload succeeded at (\\S+)").matcher(statusOutput).results()
            .map(m -> Instant.parse(m.group(1))).collect(Collectors.toList());

        assertEquals(statusOutput, transports.size(), succeeded.size());
        succeeded.forEach(t -> assertFalse(statusOutput, t.isBefore(Instant.ofEpochMilli(start))));

        Files.write(keyStore, "not a key store".getBytes());

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR, "--ssl", "reload"), "Failed to initialize key store");
    }
}
