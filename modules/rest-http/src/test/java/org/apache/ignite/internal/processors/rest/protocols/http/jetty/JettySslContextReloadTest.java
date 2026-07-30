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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.ssl.SslReloadCommandArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests that {@code --ssl reload} moves the Jetty connector serving HTTP REST onto the key store that replaced the
 * one its configuration points at.
 */
public class JettySslContextReloadTest extends GridCommonAbstractTest {
    /** System property the Jetty configuration reads the key store path from. */
    private static final String KEY_STORE_PROP = "IGNITE_TEST_JETTY_KEY_STORE";

    /** Port the Jetty configuration binds to. */
    private static final int PORT = 11443;

    /** Key store Jetty runs on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setJettyPath("modules/rest-http/src/test/resources/jetty-ssl-reload.xml"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-jetty-ssl-reload-", ".jks");

        copyKeyStore("node01");

        System.setProperty(KEY_STORE_PROP, keyStore.toString());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(KEY_STORE_PROP);

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** The HTTP REST connector must serve the rotated certificate to connections opened after the reload. */
    @Test
    public void testCertificateReloaded() throws Exception {
        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate();

        copyKeyStore("node02");

        String res = reload(g);

        assertContains(log, res, "HTTP REST");

        assertFalse("Jetty must serve the rotated certificate to new connections",
            Arrays.equals(certBefore.getEncoded(), servedCertificate().getEncoded()));
    }

    /** A key store Jetty cannot read must be reported, and the connector must keep serving the previous one. */
    @Test
    public void testBrokenKeyStoreKeepsConnectorServing() throws Exception {
        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate();

        Files.write(keyStore, "not a key store".getBytes());

        GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null);

        assertTrue("A broken key store must not reach the connector",
            Arrays.equals(certBefore.getEncoded(), servedCertificate().getEncoded()));
    }

    /**
     * @param node Node to run the reload on.
     * @return Task result.
     */
    private String reload(IgniteEx node) throws Exception {
        VisorTaskResult<String> res = node.compute(node.cluster()).execute(SslReloadTask.class,
            new VisorTaskArgument<>(Collections.singletonList(node.localNode().id()), new SslReloadCommandArg(),
                false));

        return res.result();
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void copyKeyStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * @return Certificate the connector presents on a new TLS connection.
     */
    private X509Certificate servedCertificate() throws Exception {
        SSLContext cliCtx = SSLContext.getInstance("TLS");

        cliCtx.init(null, new TrustManager[] {SslContextFactory.getDisabledTrustManager()}, null);

        try (SSLSocket sock = (SSLSocket)cliCtx.getSocketFactory()
            .createSocket(InetAddress.getLoopbackAddress(), PORT)) {

            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }
}
