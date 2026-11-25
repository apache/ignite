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
     * Then: the load succeeds with hot reload enabled and no scheduled reload date.
     */
    @Test
    public void testDefaultSettingsAcceptStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        assertTrue(factory.isHotReloadEnabled());
        assertNotNull(factory.create());
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: a certificate valid for 24 hours and a certificate valid for 90 days, both issued by a CA valid for 2 years.
     * When: the factory loads each of them.
     * Then: hot reload is due 8 hours, a third of the lifetime, before the first one expires, and 7 days before the
     *     second one expires, as a third of 90 days exceeds 7 days.
     */
    @Test
    public void testHotReloadLeadIsThirdOfLifetimeUpToSevenDays() throws Exception {
        assertHotReloadTime(at(4), authority.keyStore("node", at(-12), at(12)));
        assertHotReloadTime(at(DAYS.toHours(30 - 7)), authority.keyStore("node", at(-DAYS.toHours(60)), at(DAYS.toHours(30))));
    }

    /**
     * Given: key stores in which the certificate due first was issued 30 hours ago and expires in 15 hours.
     *     In one of them, that certificate is the issuer of a key certificate that expires sooner, in 5 hours, but is
     *     due later, in 3 hours, a third of its 6-hour lifetime before it expires.
     *     Other entries are a chain that is due later, a secret key and a key with a certificate chain that is not X.509.
     * When: the factory loads each key store.
     * Then: hot reload is due now, a third of the 45-hour lifetime before that certificate expires.
     */
    @Test
    public void testHotReloadTimeComesFromCertificateDueFirst() throws Exception {
        TestCertificateAuthority shortIssuer = new TestCertificateAuthority("short-issuer", at(-30), at(15));
        KeyStore node = authority.keyStore("node", at(-30), at(15));
        KeyStore withUnusableKeys = KeyStore.getInstance("JCEKS");

        withUnusableKeys.load(null, PASSWORD);
        withUnusableKeys.setKeyEntry("node", node.getKey("node", PASSWORD), PASSWORD, node.getCertificateChain("node"));
        withUnusableKeys.setKeyEntry("secret", new SecretKeySpec(new byte[16], "AES"), PASSWORD, null);
        withUnusableKeys.setKeyEntry("other", node.getKey("node", PASSWORD), PASSWORD,
            new Certificate[] {new OtherCertificate(node.getCertificate("node").getPublicKey())});

        assertHotReloadTime(now, node);
        assertHotReloadTime(now, shortIssuer.keyStore("node", at(-1), at(5)));
        assertHotReloadTime(now, merge(authority.keyStore("first", at(-30), at(15)), authority.keyStore("second", at(-100), at(200))));
        assertHotReloadTime(now, merge(authority.keyStore("first", at(-100), at(200)), authority.keyStore("second", at(-30), at(15))));
        assertHotReloadTime(now, withUnusableKeys);
    }

    /**
     * Given: a factory and an in-memory key store that counts its reads.
     * When: the factory loads its certificates.
     * Then: it reads the key store once, so the hot reload date matches the loaded key managers.
     */
    @Test
    public void testLoadReadsKeyStoreOnce() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-85), at(15)));

        factory.create();

        assertEquals(1, factory.keyStoreLoads);
    }

    /**
     * Given: the issuer was valid for 99 hours and expired an hour ago, and the key certificate becomes valid in 99 hours.
     * When: the factory loads the chain for the first time.
     * Then: the load succeeds, and hot reload has been due since 33 hours, a third of the issuer's lifetime, before
     *     the issuer expired.
     */
    @Test
    public void testFirstLoadAcceptsChainOutsideValidityPeriod() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", at(-100), at(-1));
        InMemorySslContextFactory factory = new InMemorySslContextFactory(expiredIssuer.keyStore("node", at(99), at(199)));

        factory.create();

        assertEquals(at(-34), factory.getHotReloadTime());
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

        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-80), at(10)));

        factory.useTrustStore(trust.toString());

        SSLContext ctx = factory.create();
        long hotReloadTime = factory.getHotReloadTime();

        factory.keyStore = authority.keyStore("node", at(-1), at(101));
        Files.delete(trust);

        IgniteException err = assertThrows(IgniteException.class, factory::init);

        assertTrue(err.getCause().getMessage(), err.getCause().getMessage().contains("key store file was not found"));
        assertSame(ctx, factory.create());
        assertEquals(hotReloadTime, factory.getHotReloadTime());

        save(authority.trustStore(), trust);
        factory.init();

        assertNotSame(ctx, factory.create());
        assertEquals(at(67), factory.getHotReloadTime());
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
     * Creates a factory that has loaded certificates due for hot reload 20 hours ago.
     * It reads both stores from memory. The certificates were issued 80 hours ago and expire in 10 hours.
     */
    private InMemorySslContextFactory dueFactory() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-80), at(10)));

        factory.trustStore = authority.trustStore();
        factory.useTrustStore("memory-trust");
        factory.create();

        assertEquals(at(-20), factory.getHotReloadTime());

        return factory;
    }

    /** Checks the hot reload time of a factory that has loaded the given key store. */
    private static void assertHotReloadTime(long expected, KeyStore keys) {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

        factory.create();

        assertEquals(expected, factory.getHotReloadTime());
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
