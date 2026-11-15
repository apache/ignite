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

import java.security.KeyStore;
import java.util.concurrent.CountDownLatch;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestThread;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.merge;
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
import static org.mockito.Mockito.spy;

/** */
public class SslContextFactoryHotReloadTest {
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
     * Given: a factory with default settings.
     * When: the test reads its hot reload flag and date.
     * Then: hot reload is disabled, and the date is zero.
     */
    @Test
    public void testHotReloadIsDisabledByDefault() {
        SslContextFactory factory = new SslContextFactory();

        assertFalse(factory.isHotReloadEnabled());
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: hot reload is disabled, and the key store holds only a CA certificate.
     * When: the factory loads the key store.
     * Then: the load succeeds.
     */
    @Test
    public void testDisabledHotReloadAcceptsStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        factory.setHotReloadEnabled(false);

        assertNotNull(factory.create());
    }

    /**
     * Given: the factory has loaded due certificates with hot reload enabled.
     * When: it reloads them with hot reload disabled, and the flag is turned back on.
     * Then: the hot reload date is zero.
     */
    @Test
    public void testLoadWithHotReloadDisabledClearsHotReloadTime() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-85), at(15)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(now, factory.getHotReloadTime());

        factory.setHotReloadEnabled(false);
        factory.init();
        factory.setHotReloadEnabled(true);

        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: key stores in which the chain that expires first has 15 of its 100 hours left.
     *     In one of them, that chain gets its expiry from an issuer that expires before the key certificate.
     *     Other entries are a chain that expires later, a chain with the same expiry and a shorter lifetime,
     *     a chain whose window opened earlier, and a secret key.
     * When: the factory loads each key store with hot reload enabled.
     * Then: the window opens now, when the chain that expires first has 15% of its lifetime left.
     */
    @Test
    public void testHotReloadTimeIsWindowStartOfEarliestExpiringChain() throws Exception {
        TestCertificateAuthority shortIssuer = new TestCertificateAuthority("short-issuer", now - DAYS.toMillis(365), at(15));
        KeyStore node = authority.keyStore("node", at(-85), at(15));
        KeyStore withSecretKey = KeyStore.getInstance("JCEKS");

        withSecretKey.load(null, PASSWORD);
        withSecretKey.setKeyEntry("node", node.getKey("node", PASSWORD), PASSWORD, node.getCertificateChain("node"));
        withSecretKey.setKeyEntry("secret", new SecretKeySpec(new byte[16], "AES"), PASSWORD, null);

        assertHotReloadWindowStartsNow(node);
        assertHotReloadWindowStartsNow(shortIssuer.keyStore("node", at(-85), at(100)));
        assertHotReloadWindowStartsNow(
            merge(authority.keyStore("first", at(-85), at(15)), authority.keyStore("second", at(-200), at(100))));
        assertHotReloadWindowStartsNow(
            merge(authority.keyStore("first", at(-200), at(100)), authority.keyStore("second", at(-85), at(15))));
        assertHotReloadWindowStartsNow(merge(authority.keyStore("first", at(-5), at(15)), authority.keyStore("second", at(-85), at(15))));
        assertHotReloadWindowStartsNow(merge(authority.keyStore("first", at(-85), at(15)), authority.keyStore("second", at(-5), at(15))));
        assertHotReloadWindowStartsNow(
            merge(authority.keyStore("first", at(-1980), at(20)), authority.keyStore("second", at(-85), at(15))));
        assertHotReloadWindowStartsNow(withSecretKey);
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
     * When: the factory loads the chain with hot reload enabled.
     * Then: the window opens at the moment the issuer expired.
     */
    @Test
    public void testExpiredChainIsDueBeforeKeyCertificateBecomesValid() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", at(-100), at(-1));
        InMemorySslContextFactory factory = new InMemorySslContextFactory(expiredIssuer.keyStore("node", at(99), at(199)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(at(-1), factory.getHotReloadTime());
    }

    /**
     * Given: hot reload is enabled, and the key store holds only a CA certificate.
     * When: the factory loads the key store.
     * Then: the load fails and asks for a private key, and the factory has no hot reload date.
     */
    @Test
    public void testEnabledHotReloadRequiresPrivateKey() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        factory.setHotReloadEnabled(true);

        IgniteException err = assertThrows(IgniteException.class, factory::create);

        assertTrue(err.getCause().getMessage().contains("private key with an X.509 certificate chain"));
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * Given: hot reload is enabled, and the certificates expired an hour ago.
     * When: the factory loads them for the first time.
     * Then: the load succeeds, and TLS peers decide whether to accept the certificates.
     */
    @Test
    public void testFirstLoadAcceptsExpiredCertificates() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-101), at(-1)));

        factory.setHotReloadEnabled(true);

        assertNotNull(factory.create());
    }

    /**
     * Given: a factory with hot reload enabled that has never loaded its certificates.
     * When: {@link SslContextFactory#hotReload()} runs.
     * Then: the factory does not read its key store.
     */
    @Test
    public void testHotReloadWithoutLoadedDateDoesNotLoad() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.setHotReloadEnabled(true);
        factory.hotReload();

        assertEquals(0, factory.keyStoreLoads);
    }

    /**
     * Given: the factory has loaded certificates whose window opens in 84 hours.
     * When: {@link SslContextFactory#hotReload()} runs.
     * Then: the factory does not read its key store.
     */
    @Test
    public void testHotReloadBeforeWindowDoesNotLoad() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-1), at(99)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(at(84), factory.getHotReloadTime());

        int loads = factory.keyStoreLoads;

        factory.hotReload();

        assertEquals(loads, factory.keyStoreLoads);
    }

    /**
     * Given: the factory has loaded due certificates, and then hot reload is turned off.
     * When: {@link SslContextFactory#hotReload()} runs.
     * Then: the factory does not read its key store.
     */
    @Test
    public void testHotReloadDoesNothingWhenDisabled() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        int loads = factory.keyStoreLoads;

        factory.setHotReloadEnabled(false);
        factory.hotReload();

        assertEquals(loads, factory.keyStoreLoads);
    }

    /**
     * Given: due certificates that expire in 10 hours, and a replacement that expires in 10 or 5 hours.
     * When: hot reload runs.
     * Then: the factory installs a new context.
     */
    @Test
    public void testHotReloadAcceptsSameOrEarlierExpiry() throws Exception {
        for (long expiry : new long[] {at(10), at(5)}) {
            InMemorySslContextFactory factory = dueFactory();
            SSLContext ctx = factory.create();

            factory.keyStore = authority.keyStore("node", at(-1), expiry);
            factory.hotReload();

            assertNotSame(ctx, factory.create());
        }
    }

    /**
     * Given: due key certificates that stay the same, and a trust store that gets a new issuer.
     * When: hot reload runs.
     * Then: the factory installs a new context.
     */
    @Test
    public void testHotReloadAcceptsTrustStoreOnlyChange() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.setHotReloadEnabled(true);
        factory.trustStore = authority.trustStore();
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("memory-trust");
        factory.setTrustStorePassword(PASSWORD);

        SSLContext ctx = factory.create();

        assertEquals(at(-5), factory.getHotReloadTime());

        TestCertificateAuthority otherIssuer = new TestCertificateAuthority("other-issuer", now - DAYS.toMillis(1), now + DAYS.toMillis(1));

        factory.trustStore.setCertificateEntry("other-issuer", otherIssuer.trustStore().getCertificate("CN=other-issuer"));
        factory.hotReload();

        assertNotSame(ctx, factory.create());
    }

    /**
     * Given: due certificates, and a replacement that has expired, is not valid yet, or comes from an expired issuer.
     * When: hot reload runs.
     * Then: the factory installs a new context without checking the validity dates.
     */
    @Test
    public void testHotReloadAcceptsCertificatesOutsideValidityPeriod() throws Exception {
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
            factory.hotReload();

            assertNotSame(ctx, factory.create());
        }
    }

    /**
     * Given: due certificates, a replacement in the source, and a missing trust store file.
     * When: hot reload runs, and then runs again with trust managers set directly.
     * Then: the first run fails and keeps the current context and date.
     *     The second run installs the replacement.
     */
    @Test
    public void testHotReloadRecoversAfterFailedTrustStoreLoad() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        SSLContext ctx = factory.create();

        factory.keyStore = authority.keyStore("node", at(-1), at(99));
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("missing-trust-store");
        factory.setTrustStorePassword(PASSWORD);

        assertHotReloadKeepsCurrentState(factory, "missing-trust-store");

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        factory.hotReload();

        assertNotSame(ctx, factory.create());
        assertEquals(at(84), factory.getHotReloadTime());
    }

    /**
     * Given: due certificates, a replacement in the source, and an unsupported protocol.
     * When: hot reload runs, and then runs again with a supported protocol.
     * Then: the first run fails after the key store loads and keeps the current context and date.
     *     The second run installs the replacement.
     */
    @Test
    public void testHotReloadRecoversAfterContextCreationFailure() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        SSLContext ctx = factory.create();

        factory.keyStore = authority.keyStore("node", at(-1), at(99));
        factory.setProtocol("UnsupportedProtocol");

        assertHotReloadKeepsCurrentState(factory, "Unsupported SSL protocol");

        factory.setProtocol("TLS");
        factory.hotReload();

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
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("memory-trust");
        factory.setTrustStorePassword(PASSWORD);

        CountDownLatch trustLoadStarted = new CountDownLatch(1);
        CountDownLatch finishTrustLoad = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("Trust source unavailable");

        doAnswer(call -> {
            trustLoadStarted.countDown();

            assertTrue(finishTrustLoad.await(TIMEOUT_MS, MILLISECONDS));

            throw failure;
        }).when(factory).loadKeyStore(any(), eq("memory-trust"), any());

        GridTestThread load = new GridTestThread(() -> assertSame(failure, assertThrows(IllegalStateException.class, factory::hotReload)));

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
     * Creates a factory with hot reload enabled that has loaded certificates whose window opened 5 hours ago.
     * The certificates have 10 of their 100 hours left. The method checks the date, so a wrong setup fails here.
     */
    private InMemorySslContextFactory dueFactory() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(at(-5), factory.getHotReloadTime());

        return factory;
    }

    /** */
    private void assertHotReloadWindowStartsNow(KeyStore keys) {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(now, factory.getHotReloadTime());
    }

    /** */
    private static void assertHotReloadKeepsCurrentState(SslContextFactory factory, String reason) {
        SSLContext ctx = factory.create();
        long hotReloadTime = factory.getHotReloadTime();
        IgniteException err = assertThrows(IgniteException.class, factory::hotReload);

        assertTrue(err.getCause().getMessage(), err.getCause().getMessage().contains(reason));
        assertSame(ctx, factory.create());
        assertEquals(hotReloadTime, factory.getHotReloadTime());
    }

    /** */
    private long at(long hours) {
        return now + HOURS.toMillis(hours);
    }

    /**
     * A factory that reads its key store from a field instead of a file and counts the reads.
     * Tests change the field to put new certificates into the source.
     * By default the factory trusts every peer. A test that sets the trust store path to {@code memory-trust}
     * and clears the trust managers gets the trust store from a field too.
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
}
