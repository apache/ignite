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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.CommandInvoker;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommand;
import org.apache.ignite.ssl.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.DFLT_JETTY_PORT;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * TLS reload of the HTTP REST connector configured in Jetty XML. The standard Jetty factory reads its key store from a file
 * and requires an SNI host name ({@code jetty/https.xml}). The custom factory ({@code jetty/https-source.xml}) takes its
 * key stores from {@link SourceSslContextFactory}. The connectors in these files set no port, so the tests set
 * {@code IGNITE_JETTY_PORT}; otherwise the node would publish port 0.
 */
@WithSystemProperty(key = IGNITE_JETTY_PORT, value = DFLT_JETTY_PORT)
public class JettyRestHttpsReloadTest extends GridCommonAbstractTest {
    /** Path read by the HTTPS connector in {@code jetty/https.xml}. */
    private static final String KEY_STORE_PATH = "ignite.test.JettyRestHttpsReloadTest.keyStorePath";

    /** Host name in the server certificates and the HTTPS requests. */
    private static final String HOST = "localhost";

    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /** */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** Current time rounded down to seconds, the precision of certificate dates. */
    private final long now = System.currentTimeMillis() / 1_000 * 1_000;

    /** Issuer of all server certificates, which the HTTPS clients trust. */
    private TestCertificateAuthority ca;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(365));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        SourceSslContextFactory.supply();

        super.afterTest();
    }

    /**
     * Given: HTTPS serves certificate A from a key store file of the standard Jetty factory.
     * When: the file gets certificate B, and the operator runs the reload command.
     * Then: the command reports HTTP REST as reloaded, and a new client sees B.
     */
    @Test
    @WithSystemProperty(key = KEY_STORE_PATH, value = "")
    public void testReloadCommandAppliesReplacementKeyStore() throws Exception {
        KeyStore a = fresh();
        KeyStore b = fresh();
        Path keys = keyStoreFile(a);

        IgniteEx node = startGrid(jettyConfig("jetty/https.xml"));

        assertEquals(a.getCertificate(HOST), serverCertificate(node));

        save(b, keys);

        assertTrue(reload(node).contains(": HTTP REST: reloaded"));
        assertEquals(b.getCertificate(HOST), serverCertificate(node));
    }

    /**
     * Given: HTTPS serves certificate A from a key store file of the standard Jetty factory, which selects certificates
     *     by SNI host name.
     * When: the file becomes unreadable and the operator runs the reload command, then the file gets certificate B and
     *     the operator runs the command again.
     * Then: the first command reports the node and the cause, and a new client that sends the host name still sees A.
     *     After the second command, a new client sees B.
     */
    @Test
    @WithSystemProperty(key = KEY_STORE_PATH, value = "")
    public void testFailedReloadKeepsLastWorkingKeys() throws Exception {
        KeyStore a = fresh();
        KeyStore b = fresh();
        Path keys = keyStoreFile(a);

        IgniteEx node = startGrid(jettyConfig("jetty/https.xml"));

        Files.writeString(keys, "not a key store");

        assertThrowsAnyCause(log, () -> reload(node), IgniteException.class, node.localNode().id() + ": failed: HTTP REST: ");
        assertEquals(a.getCertificate(HOST), serverCertificate(node));

        save(b, keys);
        reload(node);

        assertEquals(b.getCertificate(HOST), serverCertificate(node));
    }

    /**
     * Given: a custom Jetty factory starts with certificate A, which is due for hot reload, and its source then supplies B.
     * When: the node runs its hot reload check at startup, then the source fails and the operator runs the reload command.
     * Then: a new client sees B after the check. The command fails, and a new client still sees B.
     */
    @Test
    public void testHotReloadedKeysSurviveFailedManualReload() throws Exception {
        KeyStore a = due();
        KeyStore b = fresh();

        SourceSslContextFactory.supply(a, b);

        IgniteEx node = startGrid(jettyConfig("jetty/https-source.xml"));
        Certificate replacement = b.getCertificate(HOST);

        assertTrue(waitForCondition(() -> {
            try {
                return replacement.equals(serverCertificate(node));
            }
            catch (Exception e) {
                throw new AssertionError(e);
            }
        }, TIMEOUT_MS));

        SourceSslContextFactory.supply(new IOException("Source is unavailable"));

        assertThrowsAnyCause(log, () -> reload(node), IgniteException.class, "Source is unavailable");
        assertEquals(b.getCertificate(HOST), serverCertificate(node));
    }

    /**
     * Given: a custom Jetty factory has loaded certificate A, valid for 30 days.
     * When: the factory reloads by itself and gets certificate B, which was issued 20 hours ago and expires in an hour.
     *     Then its source fails, and Ignite reloads the factory.
     * Then: the hot reload date is 23 days ahead for A and 6 hours ago for B, and the failed reload keeps B's date.
     */
    @Test
    public void testHotReloadTimeFollowsKeysInUse() throws Exception {
        SourceSslContextFactory.supply(fresh(), due(), new IOException("Source is unavailable"));

        SourceSslContextFactory jettyFactory = new SourceSslContextFactory();
        ReloadableJettySslFactory factory = new ReloadableJettySslFactory(jettyFactory);

        jettyFactory.start();

        try {
            assertEquals(now + DAYS.toMillis(23), factory.getHotReloadTime());

            jettyFactory.reload(f -> { });

            assertEquals(now - HOURS.toMillis(6), factory.getHotReloadTime());
            assertThrows(log, factory::init, IgniteException.class, "Source is unavailable");
            assertEquals(now - HOURS.toMillis(6), factory.getHotReloadTime());
        }
        finally {
            jettyFactory.stop();
        }
    }

    /**
     * Given: Jetty has keys configured in memory and reads its trust store from a file.
     * When: reload fails because the file is corrupt, then the operator replaces it and retries.
     * Then: the failed reload keeps the working context. The retry picks up the new trust store with the configured keys.
     */
    @Test
    public void testTrustStoreReloadKeepsConfiguredKeysAfterFailure() throws Exception {
        KeyStore keys = fresh();
        KeyStore trusted = ca.trustStore();
        Path trust = tmp.newFile("trust.jks").toPath();
        SslContextFactory.Server jettyFactory = new SslContextFactory.Server();

        save(trusted, trust);
        jettyFactory.setKeyStore(keys);
        jettyFactory.setKeyStorePassword(new String(PASSWORD));
        jettyFactory.setTrustStorePath(trust.toString());
        jettyFactory.setTrustStoreType("JKS");
        jettyFactory.setTrustStorePassword(new String(PASSWORD));

        ReloadableJettySslFactory factory = new ReloadableJettySslFactory(jettyFactory);

        jettyFactory.start();

        try {
            SSLContext ctx = jettyFactory.getSslContext();

            Files.writeString(trust, "not a trust store");

            assertThrows(log, factory::init, IgniteException.class, null);
            assertSame(ctx, jettyFactory.getSslContext());

            Certificate cert = keys.getCertificate(HOST);

            trusted.setCertificateEntry(HOST, cert);
            save(trusted, trust);
            factory.init();

            assertSame(keys, jettyFactory.getKeyStore());
            assertEquals(cert, jettyFactory.getTrustStore().getCertificate(HOST));
        }
        finally {
            jettyFactory.stop();
        }
    }

    /**
     * Given: a custom Jetty factory that would ask its source for keys each time its reload method runs.
     * When: Ignite reloads the factory successfully.
     * Then: the reload method runs once.
     */
    @Test
    public void testReloadCallsFactoryReloadOnce() throws Exception {
        AtomicInteger reloads = new AtomicInteger();

        SslContextFactory.Server jettyFactory = new SslContextFactory.Server() {
            /** {@inheritDoc} */
            @Override public void reload(Consumer<SslContextFactory> consumer) throws Exception {
                reloads.incrementAndGet();

                super.reload(consumer);
            }
        };

        jettyFactory.setKeyStore(fresh());
        jettyFactory.setKeyStorePassword(new String(PASSWORD));

        ReloadableJettySslFactory factory = new ReloadableJettySslFactory(jettyFactory);

        jettyFactory.start();

        try {
            factory.init();

            assertEquals(1, reloads.get());
        }
        finally {
            jettyFactory.stop();
        }
    }

    /** @return Key store with a server certificate valid for 30 days, which is not due for hot reload. */
    private KeyStore fresh() throws Exception {
        return ca.keyStore(HOST, now - HOURS.toMillis(1), now + DAYS.toMillis(30));
    }

    /** @return Key store with a server certificate that expires in an hour, so it is due for hot reload. */
    private KeyStore due() throws Exception {
        return ca.keyStore(HOST, now - HOURS.toMillis(20), now + HOURS.toMillis(1));
    }

    /** Writes the key store to a file and points {@code jetty/https.xml} at it. */
    private Path keyStoreFile(KeyStore store) throws Exception {
        Path keys = tmp.newFile("server.jks").toPath();

        save(store, keys);
        System.setProperty(KEY_STORE_PATH, keys.toString());

        return keys;
    }

    /** @return Node configuration with HTTP REST configured by the given Jetty XML file. */
    private IgniteConfiguration jettyConfig(String jettyPath) throws Exception {
        return getConfiguration().setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(jettyPath));
    }

    /** Runs the reload command on the node and returns its report. */
    private static String reload(IgniteEx node) throws Exception {
        return new CommandInvoker<>(new SslReloadCommand(), new NoArg(), node).invoke(line -> { });
    }

    /**
     * Returns the certificate that the REST endpoint presents to a new client that trusts the test CA and sends
     * the host name. The JDK sends no SNI host name for {@code localhost} by itself, so the client sets it.
     */
    private Certificate serverCertificate(IgniteEx node) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(ca.trustStore());

        SSLContext client = SSLContext.getInstance("TLS");

        client.init(null, tmf.getTrustManagers(), null);

        int port = node.localNode().attribute(ATTR_REST_JETTY_PORT);

        try (SSLSocket sock = (SSLSocket)client.getSocketFactory().createSocket(HOST, port)) {
            SSLParameters params = sock.getSSLParameters();

            params.setServerNames(List.of(new SNIHostName(HOST)));
            sock.setSSLParameters(params);
            sock.startHandshake();

            return sock.getSession().getPeerCertificates()[0];
        }
    }

    /**
     * Custom Jetty factory that takes its key stores from a source in memory instead of a file.
     * Each load takes the next key store or throws the next exception that the test supplied. The last one repeats.
     */
    public static class SourceSslContextFactory extends SslContextFactory.Server {
        /** */
        private static final Deque<Object> LOADS = new ConcurrentLinkedDeque<>();

        /** Replaces what the next loads take: key stores and exceptions, in order. */
        private static void supply(Object... loads) {
            LOADS.clear();
            LOADS.addAll(List.of(loads));
        }

        /** Gives the factory an empty trust store, so that Jetty loads the key store instead of starting without keys. */
        public SourceSslContextFactory() throws Exception {
            KeyStore trust = KeyStore.getInstance("PKCS12");

            trust.load(null, null);

            setTrustStore(trust);
            setKeyStorePassword(new String(PASSWORD));
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(Resource rsrc) throws Exception {
            Object load = LOADS.size() > 1 ? LOADS.poll() : LOADS.peek();

            if (load instanceof Exception e)
                throw e;

            return (KeyStore)load;
        }
    }
}
