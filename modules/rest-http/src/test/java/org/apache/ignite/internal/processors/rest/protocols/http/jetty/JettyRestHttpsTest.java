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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.CommandInvoker;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommand;
import org.apache.ignite.ssl.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.DFLT_JETTY_PORT;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;

/** TLS reload for the HTTP REST connector configured in Jetty XML. */
@WithSystemProperty(key = IGNITE_JETTY_PORT, value = DFLT_JETTY_PORT)
public class JettyRestHttpsTest extends GridCommonAbstractTest {
    /** Path read by the HTTPS connector in jetty/https.xml. */
    private static final String KEY_STORE_PATH = "ignite.test.JettyRestHttpsTest.keyStorePath";

    /** Host name in the server certificate and the HTTPS request. */
    private static final String HOST = "localhost";

    /** */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** After the key store in Jetty XML is replaced, the reload command makes HTTPS serve its new certificate. */
    @Test
    @WithSystemProperty(key = KEY_STORE_PATH, value = "")
    public void testReloadCommandAppliesReplacementKeyStore() throws Exception {
        long now = System.currentTimeMillis();
        long from = now - DAYS.toMillis(1);
        long to = now + DAYS.toMillis(30);
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", from, to);
        KeyStore first = ca.keyStore(HOST, from, to);
        KeyStore second = ca.keyStore(HOST, from, to);
        KeyStore trust = ca.trustStore();
        Path keys = tmp.newFile("server.jks").toPath();

        save(first, keys);
        System.setProperty(KEY_STORE_PATH, keys.toString());

        ConnectorConfiguration connCfg = new ConnectorConfiguration().setJettyPath("jetty/https.xml");

        try (IgniteEx node = startGrid(getConfiguration().setConnectorConfiguration(connCfg))) {
            int port = node.localNode().attribute(ATTR_REST_JETTY_PORT);

            save(second, keys);
            new CommandInvoker<>(new SslReloadCommand(), new NoArg(), node).invoke(log::info);

            assertEquals(second.getCertificate(HOST), serverCertificate(port, trust));
        }
    }

    /** Returns the certificate presented by the REST endpoint when it serves an HTTPS request. */
    private static Certificate serverCertificate(int port, KeyStore trust) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);

        SSLContext client = SSLContext.getInstance("TLS");
        client.init(null, tmf.getTrustManagers(), null);

        HttpRequest req = HttpRequest.newBuilder(URI.create("https://" + HOST + ':' + port + "/ignite?cmd=version")).build();
        HttpResponse<Void> res = HttpClient.newBuilder().sslContext(client).build().send(req, HttpResponse.BodyHandlers.discarding());

        assertEquals(200, res.statusCode());

        return res.sslSession().orElseThrow().getPeerCertificates()[0];
    }
}
