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
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestThread;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Thread.State.BLOCKED;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** */
public class SslContextFactoryRenewalTest {
    /** */
    private final long now = System.currentTimeMillis() / 1_000 * 1_000;

    /** */
    private TestCertificateAuthority authority;

    /** */
    @Before
    public void setUp() throws Exception {
        authority = new TestCertificateAuthority("issuer", now - DAYS.toMillis(365), now + DAYS.toMillis(365));
    }

    /** */
    @Test
    public void testRenewalIsDisabledByDefault() {
        SslContextFactory factory = new SslContextFactory();

        assertFalse(factory.isRenewalEnabled());
        assertEquals(0, factory.getRenewalTime());

        factory.renew(() -> false);
    }

    /** */
    @Test
    public void testDisabledRenewalAcceptsStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        factory.setRenewalEnabled(false);
        factory.create();

        assertEquals(0, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewalWindowUsesLoadedKeyStore() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(85), now + HOURS.toMillis(15)));

        factory.create();

        assertEquals(now, factory.getRenewalTime());
        assertEquals(1, factory.keyStoreLoads);
    }

    /** */
    @Test
    public void testIssuerExpiryStartsRenewalWindow() throws Exception {
        TestCertificateAuthority issuer = new TestCertificateAuthority("issuer", now - DAYS.toMillis(365), now + HOURS.toMillis(15));
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            issuer.keyStore("node", now - HOURS.toMillis(85), now + HOURS.toMillis(100)));

        factory.create();

        assertEquals(now, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testIssuerExpiredBeforeLeafStartIsAlreadyDue() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("issuer", now - DAYS.toMillis(365), now - 1_000);
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            expiredIssuer.keyStore("node", now + DAYS.toMillis(100), now + DAYS.toMillis(200)));

        factory.create();

        assertEquals(now - 1_000, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testEarliestChainDeterminesRenewalWindow() throws Exception {
        KeyStore keys = authority.keyStore("first", now - HOURS.toMillis(85), now + HOURS.toMillis(15));
        KeyStore other = authority.keyStore("second", now - HOURS.toMillis(200), now + HOURS.toMillis(100));

        keys.setKeyEntry("second", other.getKey("second", PASSWORD), PASSWORD, other.getCertificateChain("second"));

        InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

        factory.create();

        assertEquals(now, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testEqualExpiriesUseLongestLifetime() throws Exception {
        for (String longerAlias : new String[] {"first", "second"}) {
            String shorterAlias = longerAlias.equals("first") ? "second" : "first";
            KeyStore keys = authority.keyStore(shorterAlias, now - HOURS.toMillis(5), now + HOURS.toMillis(15));
            KeyStore longer = authority.keyStore(longerAlias, now - HOURS.toMillis(85), now + HOURS.toMillis(15));

            keys.setKeyEntry(longerAlias, longer.getKey(longerAlias, PASSWORD), PASSWORD, longer.getCertificateChain(longerAlias));

            InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

            factory.create();

            assertEquals(now, factory.getRenewalTime());
        }
    }

    /** */
    @Test
    public void testSecretKeysDoNotAffectRenewalWindow() throws Exception {
        KeyStore keys = authority.keyStore("node", now - HOURS.toMillis(85), now + HOURS.toMillis(15));
        KeyStore mixed = KeyStore.getInstance("JCEKS");

        mixed.load(null, PASSWORD);
        mixed.setKeyEntry("node", keys.getKey("node", PASSWORD), PASSWORD, keys.getCertificateChain("node"));
        mixed.setKeyEntry("secret", new SecretKeySpec(new byte[16], "AES"), PASSWORD, null);

        InMemorySslContextFactory factory = new InMemorySslContextFactory(mixed);

        factory.create();

        assertEquals(now, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testEnabledRenewalRequiresPrivateKey() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());
        IgniteException err = assertThrows(IgniteException.class, factory::create);

        assertTrue(err.getCause().getMessage().contains("private key with an X.509 certificate chain"));
        assertEquals(0, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewBeforeWindowDoesNotLoad() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(1), now + HOURS.toMillis(99)));
        factory.create();
        factory.renew(() -> false);

        assertEquals(1, factory.keyStoreLoads);
    }

    /** */
    @Test
    public void testCancellationWhileWaitingForFactorySkipsRenewal() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
        SSLContext ctx = factory.create();

        factory.keyStore = authority.keyStore("replacement", now - HOURS.toMillis(1), now + HOURS.toMillis(99));

        AtomicBoolean cancelled = new AtomicBoolean();
        GridTestThread attempt = new GridTestThread(() -> factory.renew(cancelled::get));

        try {
            synchronized (factory) {
                attempt.start();

                long deadline = System.currentTimeMillis() + 5_000;

                assertTrue(waitForCondition(() -> attempt.getState() == BLOCKED, () -> System.currentTimeMillis() < deadline));

                cancelled.set(true);
            }
        }
        finally {
            attempt.join(5_000);
        }

        assertFalse(attempt.isAlive());
        attempt.checkError();
        assertEquals(1, factory.keyStoreLoads);
        assertSame(ctx, factory.create());
    }

    /** */
    @Test
    public void testRenewReplacesExpiredContext() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(101), now - HOURS.toMillis(1)));
        SSLContext ctx = factory.create();

        factory.keyStore = authority.keyStore("node", now - HOURS.toMillis(1), now + HOURS.toMillis(99));
        factory.renew(() -> false);

        assertNotSame(ctx, factory.create());
        assertEquals(now + HOURS.toMillis(84), factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewRejectsUnchangedExpiry() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("first", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
        SSLContext ctx = factory.create();
        long renewalTime = factory.getRenewalTime();

        factory.keyStore = authority.keyStore("replacement", now - HOURS.toMillis(80), now + HOURS.toMillis(10));

        IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

        assertTrue(err.getCause().getMessage().contains("later earliest expiry"));
        assertSame(ctx, factory.create());
        assertEquals(renewalTime, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewRejectsExpiredIssuer() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
        SSLContext ctx = factory.create();
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", now - HOURS.toMillis(100), now - 1_000);

        factory.keyStore = expiredIssuer.keyStore("replacement", now - HOURS.toMillis(1), now + HOURS.toMillis(99));

        IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

        assertTrue(err.getCause().getMessage().contains("subject=CN=expired-issuer"));
        assertSame(ctx, factory.create());
    }

    /** */
    @Test
    public void testRenewRejectsFutureCertificateInOtherChain() throws Exception {
        for (String notYetValidAlias : new String[] {"first", "second"}) {
            InMemorySslContextFactory factory = new InMemorySslContextFactory(
                authority.keyStore("node", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
            SSLContext ctx = factory.create();
            String validAlias = notYetValidAlias.equals("first") ? "second" : "first";
            KeyStore replacement = authority.keyStore(validAlias, now - HOURS.toMillis(1), now + HOURS.toMillis(99));
            KeyStore notYetValidKeys = authority.keyStore(notYetValidAlias, now + HOURS.toMillis(1), now + HOURS.toMillis(100));

            replacement.setKeyEntry(notYetValidAlias, notYetValidKeys.getKey(notYetValidAlias, PASSWORD), PASSWORD,
                notYetValidKeys.getCertificateChain(notYetValidAlias));
            factory.keyStore = replacement;

            IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

            assertTrue(err.getCause().getMessage().contains("alias=" + notYetValidAlias));
            assertSame(ctx, factory.create());
        }
    }

    /** */
    @Test
    public void testManualReloadAcceptsEarlierExpiryAfterRenewalFailure() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
        SSLContext ctx = factory.create();

        IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

        assertTrue(err.getCause().getMessage().contains("later earliest expiry"));

        factory.keyStore = authority.keyStore("node", now - HOURS.toMillis(101), now - HOURS.toMillis(1));
        factory.init();

        assertNotSame(ctx, factory.create());
        assertEquals(now - HOURS.toMillis(16), factory.getRenewalTime());
    }

    /** */
    @Test
    public void testFailedTrustStoreLoadKeepsContextAndRenewalWindow() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(
            authority.keyStore("node", now - HOURS.toMillis(90), now + HOURS.toMillis(10)));
        SSLContext ctx = factory.create();
        long renewalTime = factory.getRenewalTime();

        factory.keyStore = authority.keyStore("node", now - HOURS.toMillis(1), now + HOURS.toMillis(99));
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("missing-trust-store");
        factory.setTrustStorePassword(PASSWORD);

        IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

        assertTrue(err.getCause().getMessage().contains("missing-trust-store"));
        assertSame(ctx, factory.create());
        assertEquals(renewalTime, factory.getRenewalTime());

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        factory.renew(() -> false);

        assertNotSame(ctx, factory.create());
        assertEquals(now + HOURS.toMillis(84), factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewRejectsCertificatesExpiredDuringTrustStoreLoad() throws Exception {
        KeyStore trustStore = authority.trustStore();
        KeyStore curKeys = authority.keyStore("expired", now - HOURS.toMillis(101), now - HOURS.toMillis(1));
        KeyStore validKeys = authority.keyStore("valid", now - HOURS.toMillis(1), now + HOURS.toMillis(99));

        curKeys.setKeyEntry("valid", validKeys.getKey("valid", PASSWORD), PASSWORD, validKeys.getCertificateChain("valid"));

        /** */
        class SlowTrustStoreFactory extends InMemorySslContextFactory {
            /** */
            private static final long serialVersionUID = 0L;

            /** */
            private long replacementExpiry;

            /** */
            private SlowTrustStoreFactory() {
                super(curKeys);
            }

            /** {@inheritDoc} */
            @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) throws SSLException {
                if (!"slow-trust-store".equals(path))
                    return super.loadKeyStore(type, path, pwd);

                try {
                    long deadline = System.currentTimeMillis() + 10_000;

                    assertTrue(waitForCondition(() -> System.currentTimeMillis() > replacementExpiry,
                        () -> System.currentTimeMillis() < deadline));
                }
                catch (IgniteInterruptedCheckedException e) {
                    Thread.currentThread().interrupt();

                    throw new SSLException(e);
                }

                return trustStore;
            }
        }

        SlowTrustStoreFactory factory = new SlowTrustStoreFactory();
        SSLContext ctx = factory.create();
        long renewalTime = factory.getRenewalTime();

        factory.replacementExpiry = System.currentTimeMillis() / 1_000 * 1_000 + 5_000;
        factory.keyStore = authority.keyStore("replacement", now - HOURS.toMillis(1), factory.replacementExpiry);
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("slow-trust-store");
        factory.setTrustStorePassword(PASSWORD);

        IgniteException err = assertThrows(IgniteException.class, () -> factory.renew(() -> false));

        assertTrue(err.getCause().getMessage().contains("alias=replacement"));
        ((X509Certificate)curKeys.getCertificate("valid")).checkValidity();
        assertSame(ctx, factory.create());
        assertEquals(renewalTime, factory.getRenewalTime());
    }

    /** */
    private static class InMemorySslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected KeyStore keyStore;

        /** */
        private int keyStoreLoads;

        /** */
        private InMemorySslContextFactory(KeyStore keyStore) {
            this.keyStore = keyStore;

            setKeyStoreFilePath("memory");
            setKeyStorePassword(PASSWORD);
            setTrustManagers(getDisabledTrustManager());
            setRenewalEnabled(true);
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) throws SSLException {
            if (!"memory".equals(path))
                return super.loadKeyStore(type, path, pwd);

            keyStoreLoads++;

            return keyStore;
        }
    }
}
