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

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Tests {@code --ssl reload} as an operator runs it, through the command line handler: what it asks, what it changes on
 * the nodes and what it reports.
 */
public class GridCommandHandlerSslReloadTest extends GridCommandHandlerAbstractTest {
    /** Key store the nodes run on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(cliCommandHandler());

        keyStore = Files.createTempFile("ignite-cli-ssl-reload-", ".jks");

        copyKeyStore("node01");

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // The base keeps the cluster for the next test, but every test here starts from the certificate it places.
        stopAllGrids();

        autoConfirmation = true;

        super.afterTest();

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /**
     * One invocation has to reach every node of the cluster, client nodes included, put the certificates on disk in
     * use there, and report what each of them serves.
     */
    @Test
    public void testReloadAppliesAndReports() throws Exception {
        startGrids(2);

        IgniteEx cli = startClientGrid(2);

        copyKeyStore("node02");

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--ssl", "reload"));

        String out = testOut.toString();

        for (IgniteEx node : F.asList(grid(0), grid(1), cli))
            assertContains(log, out, node.localNode().id() + ": reloaded ");

        // The binary REST connector shares the factory of the node, so it is reloaded and reported with it.
        assertContains(log, out, "reloaded binary REST, client connector, communication, discovery");
        assertContains(log, out, "serving CN=node02");

        assertEquals("CN=node02", servedCertificate(grid(0)).getSubjectX500Principal().getName());
        assertEquals("CN=node02", servedCertificate(grid(1)).getSubjectX500Principal().getName());
    }

    /** Answering the question with no must leave every node on the certificate it is running. */
    @Test
    public void testDeclinedConfirmationAppliesNothing() throws Exception {
        startGrids(2);

        copyKeyStore("node02");

        autoConfirmation = false;

        injectTestSystemOut();
        injectTestSystemIn("n");

        assertEquals(EXIT_CODE_OK, execute("--ssl", "reload"));

        String out = testOut.toString();

        assertContains(log, out, "Operation cancelled");
        assertNotContains(log, out, ": reloaded ");

        assertEquals("CN=node01", servedCertificate(grid(0)).getSubjectX500Principal().getName());
    }

    /** A reload that failed on a node must fail the command, name the reason, and leave that node as it was. */
    @Test
    public void testFailureReturnsError() throws Exception {
        startGrids(2);

        Files.write(keyStore, "not a key store".getBytes());

        injectTestSystemOut();

        assertFalse("A failed reload must not end with success", execute("--ssl", "reload") == EXIT_CODE_OK);

        String out = testOut.toString();

        assertContains(log, out, "failed on");

        // The reason names the store that could not be read.
        assertContains(log, out, "Failed to initialize key store");

        assertEquals("CN=node01", servedCertificate(grid(0)).getSubjectX500Principal().getName());
    }

    /** A dry run has nothing to confirm: it must report and stop without asking. */
    @Test
    public void testDryRunDoesNotAsk() throws Exception {
        startGrids(2);

        copyKeyStore("node02");

        autoConfirmation = false;

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--ssl", "reload", "--dry-run"));

        String out = testOut.toString();

        assertContains(log, out, "can be reloaded");

        // Nothing was cancelled, because nothing was asked.
        assertNotContains(log, out, "Operation cancelled");
        assertNotContains(log, out, ": reloaded ");

        assertEquals("CN=node01", servedCertificate(grid(0)).getSubjectX500Principal().getName());
    }

    /** Status must report what every node serves and end with success when nothing is wrong. */
    @Test
    public void testStatus() throws Exception {
        startGrids(2);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--ssl", "status"));

        String out = testOut.toString();

        for (int i = 0; i < 2; i++)
            assertContains(log, out, grid(i).localNode().id() + ": binary REST, client connector, communication, discovery");

        assertContains(log, out, "serving CN=node01");
    }

    /**
     * @param node Node whose client connector is probed.
     * @return Certificate the connector presents on a new TLS connection.
     */
    private X509Certificate servedCertificate(IgniteEx node) throws Exception {
        SSLContext cliCtx = SSLContext.getInstance("TLS");

        cliCtx.init(null, new TrustManager[] {SslContextFactory.getDisabledTrustManager()}, null);

        try (SSLSocket sock = (SSLSocket)cliCtx.getSocketFactory()
            .createSocket(InetAddress.getLoopbackAddress(), node.context().clientListener().port())) {

            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void copyKeyStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }
}
