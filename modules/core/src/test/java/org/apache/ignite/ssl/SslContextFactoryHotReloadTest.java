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

package org.apache.ignite.ssl;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestThread;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.merge;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** */
public class SslContextFactoryHotReloadTest {
    /** */
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /** */
    private final long now = System.currentTimeMillis() / 1_000 * 1_000;

    /** */
    private TestCertificateAuthority authority;

    /**
     * Each test gets a test CA whose certificate is valid from a year before to a year after {@code now}.
     * The tests issue their certificates from this CA with dates counted from {@code now}.
     */
    @Before
    public void setUp() throws Exception {
        authority = new TestCertificateAuthority("issuer", now - DAYS.toMillis(365), now + DAYS.toMillis(365));
    }

    /**
     * Given: a key store with only a CA certificate and the default hot reload setting.
     * When: the factory loads the store.
     * Then: the load succeeds with hot reload disabled and no scheduled reload date.
     */
    @Test
    public void testDefaultSettingsAcceptStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        assertFalse(factory.isHotReloadEnabled());
        assertNotNull(factory.create());
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: a factory has loaded a certificate that expires in 10 days.
     * When: the application increases the lead time from the default of 7 days to 14 days before starting the node.
     * Then: the same certificate is already due for hot reload.
     */
    @Test
    public void testHotReloadTimeUsesCurrentLeadTime() throws Exception {
        Path keys = tmp.newFile("keys.jks").toPath();

        save(authority.keyStore("node", at(-1), at(DAYS.toHours(10))), keys);

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(PASSWORD);
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        factory.create();

        assertEquals(at(DAYS.toHours(3)), factory.getHotReloadTime());

        factory.setHotReloadBeforeExpiryMs(DAYS.toMillis(14));

        assertEquals(at(-DAYS.toHours(4)), factory.getHotReloadTime());
    }

    /**
     * Given: a factory with the default lead time.
     * When: the application sets a zero or negative lead time.
     * Then: the setter rejects it and keeps the default.
     */
    @Test
    public void testNonPositiveHotReloadBeforeExpiryMsIsRejected() {
        SslContextFactory factory = new SslContextFactory();

        assertThrows(IllegalArgumentException.class, () -> factory.setHotReloadBeforeExpiryMs(0));
        assertThrows(IllegalArgumentException.class, () -> factory.setHotReloadBeforeExpiryMs(-1));
        assertEquals(DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS, factory.getHotReloadBeforeExpiryMs());
    }

    /**
     * Given: key stores in which the chain that expires first has 15 hours left.
     *     In one of them, that chain gets its expiry from an issuer that expires before the key certificate.
     *     Other entries are a chain that expires later, a secret key and a key with a certificate chain that is not X.509.
     * When: the factory loads each key store with hot reload enabled.
     * Then: hot reload is due now, 15 hours before the chain that expires first.
     */
    @Test
    public void testHotReloadTimeComesFromEarliestExpiringChain() throws Exception {
        TestCertificateAuthority shortIssuer = new TestCertificateAuthority("short-issuer", now - DAYS.toMillis(365), at(15));
        KeyStore node = authority.keyStore("node", at(-85), at(15));
        KeyStore withUnusableKeys = KeyStore.getInstance("JCEKS");

        withUnusableKeys.load(null, PASSWORD);
        withUnusableKeys.setKeyEntry("node", node.getKey("node", PASSWORD), PASSWORD, node.getCertificateChain("node"));
        withUnusableKeys.setKeyEntry("secret", new SecretKeySpec(new byte[16], "AES"), PASSWORD, null);
        withUnusableKeys.setKeyEntry("other", node.getKey("node", PASSWORD), PASSWORD,
            new Certificate[] {new OtherCertificate(node.getCertificate("node").getPublicKey())});

        assertHotReloadIsDueNow(node);
        assertHotReloadIsDueNow(shortIssuer.keyStore("node", at(-85), at(100)));
        assertHotReloadIsDueNow(merge(authority.keyStore("first", at(-85), at(15)), authority.keyStore("second", at(-200), at(100))));
        assertHotReloadIsDueNow(merge(authority.keyStore("first", at(-200), at(100)), authority.keyStore("second", at(-85), at(15))));
        assertHotReloadIsDueNow(withUnusableKeys);
    }

    /**
     * Given: a factory with hot reload enabled and an in-memory key store that counts its reads.
     * When: the factory loads its certificates.
     * Then: it reads the key store once, so the hot reload date matches the loaded key managers.
     */
    @Test
    public void testLoadReadsKeyStoreOnce() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-85), at(15)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(1, factory.keyStoreLoads);
    }

    /**
     * Given: the issuer expired an hour ago, and the key certificate becomes valid in 99 hours.
     * When: the factory loads the chain for the first time with hot reload enabled.
     * Then: the load succeeds, and hot reload has been due since 15 hours before the issuer expired.
     */
    @Test
    public void testFirstLoadAcceptsChainOutsideValidityPeriod() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", at(-100), at(-1));
        InMemorySslContextFactory factory = new InMemorySslContextFactory(expiredIssuer.keyStore("node", at(99), at(199)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(at(-16), factory.getHotReloadTime());
    }

    /**
     * Given: the key store returns a certificate chain for an entry without a private key, as Windows-MY does.
     * When: the factory loads the key store.
     * Then: the load succeeds, and the factory has no hot reload date.
     */
    @Test
    public void testStoreWithoutPrivateKeyLoadsWithoutHotReloadDate() throws Exception {
        KeyStore store = spy(authority.trustStore());
        String alias = store.aliases().nextElement();

        doReturn(new Certificate[] {store.getCertificate(alias)}).when(store).getCertificateChain(alias);

        InMemorySslContextFactory factory = new InMemorySslContextFactory(store);

        factory.setHotReloadEnabled(true);

        assertNotNull(factory.create());
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: the server certificate expires in 10 hours, and the client trusts only the issuer of its replacement.
     * When: the server reloads a certificate that expires in 10 or 5 hours.
     * Then: the client accepts the server's replacement certificate and completes the TLS handshake.
     */
    @Test
    public void testReloadAcceptsSameOrEarlierExpiry() throws Exception {
        TestCertificateAuthority replacementIssuer = new TestCertificateAuthority("replacement-issuer", at(-24), at(24));
        InMemorySslContextFactory client = new InMemorySslContextFactory(authority.keyStore("client", at(-24), at(24)));

        client.trustStore = replacementIssuer.trustStore();
        client.useTrustStore("memory-trust");

        SSLContext clientCtx = client.create();

        for (long expiry : new long[] {at(10), at(5)}) {
            InMemorySslContextFactory factory = dueFactory();

            factory.keyStore = replacementIssuer.keyStore("node", at(-1), expiry);
            factory.init();

            assertTrue(serverAccepts(factory.create(), clientCtx));
        }
    }

    /**
     * Given: a server keeps its keys, and a client presents a certificate from a CA that the server does not trust yet.
     * When: only the trust store gets that CA, and the factory reloads.
     * Then: the context loaded before the change rejects the client, and the reloaded context accepts it.
     */
    @Test
    public void testReloadTrustsCaAddedToTrustStore() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        SSLContext ctx = factory.create();
        TestCertificateAuthority otherIssuer = new TestCertificateAuthority("other-issuer", now - DAYS.toMillis(1), now + DAYS.toMillis(1));
        InMemorySslContextFactory client = new InMemorySslContextFactory(otherIssuer.keyStore("client", at(-1), at(23)));

        client.trustStore = authority.trustStore();
        client.useTrustStore("memory-trust");

        SSLContext clientCtx = client.create();

        assertFalse(serverAccepts(ctx, clientCtx));

        factory.trustStore.setCertificateEntry("other-issuer", otherIssuer.trustStore().getCertificate("CN=other-issuer"));
        factory.init();

        assertTrue(serverAccepts(factory.create(), clientCtx));
    }

    /**
     * Given: due certificates, and a replacement that has expired, is not valid yet, or comes from an expired issuer.
     * When: the factory reloads.
     * Then: the factory installs a new context without checking the validity dates.
     */
    @Test
    public void testReloadAcceptsCertificatesOutsideValidityPeriod() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", at(-100), at(-1));

        KeyStore[] replacements = {
            authority.keyStore("node", at(-101), at(-1)),
            authority.keyStore("node", at(1), at(101)),
            expiredIssuer.keyStore("node", at(-1), at(99))
        };

        for (KeyStore replacement : replacements) {
            InMemorySslContextFactory factory = dueFactory();
            SSLContext ctx = factory.create();

            factory.keyStore = replacement;
            factory.init();

            assertNotSame(ctx, factory.create());
        }
    }

    /**
     * Given: the factory has loaded due keys and a trust store file.
     * When: the trust store disappears during key rotation, then returns at its configured path.
     * Then: the failed reload keeps the previous context and date. The retry installs the replacement.
     */
    @Test
    public void testReloadRecoversAfterFailedTrustStoreLoad() throws Exception {
        Path trust = tmp.newFile("trust.jks").toPath();

        save(authority.trustStore(), trust);

        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.useTrustStore(trust.toString());
        factory.setHotReloadEnabled(true);

        SSLContext ctx = factory.create();
        long hotReloadTime = factory.getHotReloadTime();

        factory.keyStore = authority.keyStore("node", at(-1), at(99));
        Files.delete(trust);

        IgniteException err = assertThrows(IgniteException.class, factory::init);

        assertTrue(err.getCause().getMessage(), err.getCause().getMessage().contains("key store file was not found"));
        assertSame(ctx, factory.create());
        assertEquals(hotReloadTime, factory.getHotReloadTime());

        save(authority.trustStore(), trust);
        factory.init();

        assertNotSame(ctx, factory.create());
        assertEquals(at(84), factory.getHotReloadTime());
    }

    /**
     * Given: a reload of certificates with a new date blocks on the trust store load.
     * When: another thread reads {@link SslContextFactory#getHotReloadTime()} during the reload.
     * Then: the read returns the current date without waiting for the reload.
     */
    @Test
    public void testHotReloadTimeRemainsReadableDuringLoad() throws Exception {
        InMemorySslContextFactory factory = spy(dueFactory());
        long hotReloadTime = factory.getHotReloadTime();

        factory.keyStore = authority.keyStore("node", at(-1), at(99));

        CountDownLatch trustLoadStarted = new CountDownLatch(1);
        CountDownLatch finishTrustLoad = new CountDownLatch(1);

        doAnswer(call -> {
            trustLoadStarted.countDown();

            assertTrue(finishTrustLoad.await(TIMEOUT_MS, MILLISECONDS));

            return call.callRealMethod();
        }).when(factory).loadKeyStore(any(), eq("memory-trust"), any());

        GridTestThread load = new GridTestThread(factory::init);

        load.start();

        try {
            assertTrue(trustLoadStarted.await(TIMEOUT_MS, MILLISECONDS));

            assertEquals(hotReloadTime, factory.getHotReloadTime());
        }
        finally {
            finishTrustLoad.countDown();
            load.join(TIMEOUT_MS);
        }

        assertFalse(load.isAlive());
        load.checkError();
    }

    /**
     * Creates a factory with hot reload enabled that has loaded certificates due for hot reload 5 hours ago.
     * It reads both stores from memory. The certificates expire in 10 hours.
     */
    private InMemorySslContextFactory dueFactory() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.trustStore = authority.trustStore();
        factory.useTrustStore("memory-trust");
        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(at(-5), factory.getHotReloadTime());

        return factory;
    }

    /** */
    private void assertHotReloadIsDueNow(KeyStore keys) {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(now, factory.getHotReloadTime());
    }

    /**
     * Connects a client to a server that requires a client certificate and reports whether the server accepted it.
     * The server side decides, because with TLS 1.3 the client can finish its part of the handshake first.
     */
    private static boolean serverAccepts(SSLContext server, SSLContext client) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();

        try (SSLServerSocket srvSock = (SSLServerSocket)server.getServerSocketFactory().createServerSocket(0, 1, loopback)) {
            srvSock.setNeedClientAuth(true);
            srvSock.setSoTimeout((int)TIMEOUT_MS);

            FutureTask<Boolean> accept = new FutureTask<>(() -> {
                try (SSLSocket sock = (SSLSocket)srvSock.accept()) {
                    sock.setSoTimeout((int)TIMEOUT_MS);
                    sock.startHandshake();

                    return true;
                }
                catch (SSLHandshakeException e) {
                    return false;
                }
            });

            new GridTestThread(accept).start();

            try (SSLSocket sock = (SSLSocket)client.getSocketFactory().createSocket(loopback, srvSock.getLocalPort())) {
                sock.setSoTimeout((int)TIMEOUT_MS);
                sock.startHandshake();
            }
            catch (IOException ignored) {
                // No-op.
            }

            return accept.get(TIMEOUT_MS, MILLISECONDS);
        }
    }

    /** */
    private long at(long hours) {
        return now + HOURS.toMillis(hours);
    }

    /**
     * A factory that reads its key store from a field instead of a file and counts the reads.
     * Tests change the field to put new certificates into the source.
     * By default the factory trusts all peers. Tests can configure a trust store from memory or a file.
     * Hot reload is due 15 hours before expiry, so the tests count certificate dates in hours.
     */
    private static class InMemorySslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private KeyStore keyStore;

        /** */
        private KeyStore trustStore;

        /** */
        private int keyStoreLoads;

        /** */
        private InMemorySslContextFactory(KeyStore keyStore) {
            this.keyStore = keyStore;

            setKeyStoreFilePath("memory");
            setKeyStorePassword(PASSWORD);
            setTrustManagers(getDisabledTrustManager());
            setHotReloadBeforeExpiryMs(HOURS.toMillis(15));
        }

        /** Selects the trust store that supplies the factory's trust managers. */
        private void useTrustStore(String path) {
            setTrustManagers((TrustManager[])null);
            setTrustStoreFilePath(path);
            setTrustStorePassword(PASSWORD);
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) throws SSLException {
            if ("memory-trust".equals(path))
                return trustStore;

            if (!"memory".equals(path))
                return super.loadKeyStore(type, path, pwd);

            keyStoreLoads++;

            return keyStore;
        }
    }

    /** A certificate of a type other than X.509, which TLS key managers skip. */
    private static class OtherCertificate extends Certificate {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final PublicKey key;

        /** */
        private OtherCertificate(PublicKey key) {
            super("Other");

            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public byte[] getEncoded() {
            return new byte[0];
        }

        /** {@inheritDoc} */
        @Override public void verify(PublicKey key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void verify(PublicKey key, String sigProvider) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "OtherCertificate";
        }

        /** {@inheritDoc} */
        @Override public PublicKey getPublicKey() {
            return key;
        }
    }
}
