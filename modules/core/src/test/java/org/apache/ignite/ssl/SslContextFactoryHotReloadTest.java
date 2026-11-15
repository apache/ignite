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

    /** */
    @Before
    public void setUp() throws Exception {
        authority = new TestCertificateAuthority("issuer", now - DAYS.toMillis(365), now + DAYS.toMillis(365));
    }

    /**
     * A new factory has hot reload disabled and no hot reload date.
     * The test reads the flag and the date of a factory with default settings.
     */
    @Test
    public void testHotReloadIsDisabledByDefault() {
        SslContextFactory factory = new SslContextFactory();

        assertFalse(factory.isHotReloadEnabled());
        assertEquals(0, factory.getHotReloadTime());
    }

    /**
     * With hot reload disabled, a key store without private keys loads.
     * The test loads a key store that holds only a CA certificate.
     */
    @Test
    public void testDisabledHotReloadAcceptsStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        factory.setHotReloadEnabled(false);

        assertNotNull(factory.create());
    }

    /**
     * A load with hot reload disabled clears the hot reload date of an earlier load.
     * The test loads due certificates with hot reload enabled and then reloads them with it disabled.
     * It turns the flag back on and checks that the date stays zero.
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
     * The hot reload window opens when the chain that expires first has 15% of its lifetime left.
     * In each key store, that chain has 15 of its 100 hours left, so the test expects the window to open now.
     * One key store has an issuer that expires before its key certificate and so sets the chain expiry.
     * The other entries must not move the window: a chain that expires later, a chain with the same expiry
     * and a shorter lifetime, a chain whose window opened earlier, and a secret key.
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
     * A load reads the key store once, so the hot reload date matches the loaded key managers.
     * The test counts the reads of the in-memory key store during one load.
     */
    @Test
    public void testLoadReadsKeyStoreOnce() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-85), at(15)));

        factory.setHotReloadEnabled(true);
        factory.create();

        assertEquals(1, factory.keyStoreLoads);
    }

    /**
     * If the issuer expires before the key certificate becomes valid, the window opens when the issuer expires.
     * The test loads a chain whose issuer expired an hour ago and whose key certificate becomes valid in 99 hours.
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
     * With hot reload enabled, a key store without a private key fails to load and leaves no hot reload date.
     * The test loads a key store that holds only a CA certificate and checks the error and the date.
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
     * With hot reload enabled, the first load accepts expired certificates and leaves the decision to TLS peers.
     * The test loads certificates that expired an hour ago.
     */
    @Test
    public void testFirstLoadAcceptsExpiredCertificates() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-101), at(-1)));

        factory.setHotReloadEnabled(true);

        assertNotNull(factory.create());
    }

    /**
     * {@link SslContextFactory#hotReload()} does not load a factory that has never loaded its certificates.
     * The test enables hot reload on a new factory, calls hotReload() and checks that the key store was not read.
     */
    @Test
    public void testHotReloadWithoutLoadedDateDoesNotLoad() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.setHotReloadEnabled(true);
        factory.hotReload();

        assertEquals(0, factory.keyStoreLoads);
    }

    /**
     * {@link SslContextFactory#hotReload()} does not load new certificates before the window opens.
     * The test loads certificates whose window opens in 84 hours and checks that hotReload() reads nothing.
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
     * {@link SslContextFactory#hotReload()} does nothing after hot reload is turned off, even if the date is due.
     * The test loads due certificates with hot reload enabled, turns the flag off and checks that hotReload() reads nothing.
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
     * Hot reload installs a replacement that expires at the same time as the current certificates or earlier.
     * The current certificates expire in 10 hours, and the test reloads replacements that expire in 10 and 5 hours.
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
     * Hot reload installs a new context when only the trust store changes.
     * The test loads due key certificates, adds a new trusted issuer and checks that hotReload() installs a new context.
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
     * Hot reload installs replacements without checking their validity dates.
     * The test reloads an expired certificate, a certificate that is not valid yet, and a certificate of an expired issuer.
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
     * A failed trust store load keeps the current context and date, and the next attempt installs the replacement.
     * The test fails the first attempt with a missing trust store file and lets the second use trust managers set directly.
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
     * A failure to create the SSL context keeps the current context and date, and the next attempt installs the replacement.
     * The test fails the first attempt with an unsupported protocol after the key store loads and then restores the protocol.
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
     * {@link SslContextFactory#getHotReloadTime()} returns the current date while a reload is in progress.
     * The test starts a reload of certificates with a new date, blocks it on the trust store load and reads the date.
     * Then it lets the load fail.
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

    /** @return Factory that has loaded certificates whose hot reload window has started. */
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

    /** */
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
