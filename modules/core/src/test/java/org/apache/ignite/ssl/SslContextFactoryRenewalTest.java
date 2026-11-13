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
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.merge;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

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

        factory.renew();
    }

    /** */
    @Test
    public void testDisabledRenewalAcceptsStoreWithoutPrivateKeys() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.trustStore());

        factory.setRenewalEnabled(false);
        factory.create();
        factory.init();

        assertEquals(0, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testLoadWithRenewalDisabledClearsRenewalTime() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-85), at(15)));

        factory.create();
        factory.setRenewalEnabled(false);
        factory.init();
        factory.setRenewalEnabled(true);

        assertEquals(0, factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewalTimeIsWindowStartOfEarliestExpiringChain() throws Exception {
        TestCertificateAuthority shortIssuer = new TestCertificateAuthority("short-issuer", now - DAYS.toMillis(365), at(15));
        KeyStore node = authority.keyStore("node", at(-85), at(15));
        KeyStore withSecretKey = KeyStore.getInstance("JCEKS");

        withSecretKey.load(null, PASSWORD);
        withSecretKey.setKeyEntry("node", node.getKey("node", PASSWORD), PASSWORD, node.getCertificateChain("node"));
        withSecretKey.setKeyEntry("secret", new SecretKeySpec(new byte[16], "AES"), PASSWORD, null);

        assertRenewalWindowStartsNow(node);
        assertRenewalWindowStartsNow(shortIssuer.keyStore("node", at(-85), at(100)));
        assertRenewalWindowStartsNow(merge(authority.keyStore("first", at(-85), at(15)), authority.keyStore("second", at(-200), at(100))));
        assertRenewalWindowStartsNow(merge(authority.keyStore("first", at(-200), at(100)), authority.keyStore("second", at(-85), at(15))));
        assertRenewalWindowStartsNow(merge(authority.keyStore("first", at(-5), at(15)), authority.keyStore("second", at(-85), at(15))));
        assertRenewalWindowStartsNow(merge(authority.keyStore("first", at(-85), at(15)), authority.keyStore("second", at(-5), at(15))));
        assertRenewalWindowStartsNow(withSecretKey);
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
    public void testFirstLoadRejectsExpiredCertificates() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-101), at(-1)));

        factory.setRenewalEnabled(false);

        IgniteException err = assertThrows(IgniteException.class, factory::create);

        assertTrue(err.getCause().getMessage().contains("not currently valid"));
    }

    /** */
    @Test
    public void testRenewBeforeWindowDoesNotLoad() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-1), at(99)));

        factory.create();
        factory.renew();

        assertEquals(1, factory.keyStoreLoads);
    }

    /** */
    @Test
    public void testRenewAcceptsAnyValidReplacement() throws Exception {
        for (long expiry : new long[] {at(10), at(5)}) {
            InMemorySslContextFactory factory = dueFactory();
            SSLContext ctx = factory.create();

            factory.keyStore = authority.keyStore("node", at(-1), expiry);
            factory.renew();

            assertNotSame(ctx, factory.create());
        }

        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.trustStore = authority.trustStore();
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("memory-trust");
        factory.setTrustStorePassword(PASSWORD);

        SSLContext ctx = factory.create();
        TestCertificateAuthority otherIssuer = new TestCertificateAuthority("other-issuer", now - DAYS.toMillis(1), now + DAYS.toMillis(1));

        factory.trustStore.setCertificateEntry("other-issuer", otherIssuer.trustStore().getCertificate("CN=other-issuer"));
        factory.renew();

        assertNotSame(ctx, factory.create());
    }

    /** */
    @Test
    public void testRenewRejectsCertificatesThatAreNotValid() throws Exception {
        TestCertificateAuthority expiredIssuer = new TestCertificateAuthority("expired-issuer", at(-100), now - 1_000);
        InMemorySslContextFactory factory = dueFactory();

        factory.keyStore = expiredIssuer.keyStore("replacement", at(-1), at(99));
        assertRenewKeepsCurrentState(factory, "subject=CN=expired-issuer");

        factory.keyStore = merge(authority.keyStore("first", at(1), at(100)), authority.keyStore("second", at(-1), at(99)));
        assertRenewKeepsCurrentState(factory, "alias=first");

        factory.keyStore = merge(authority.keyStore("first", at(-1), at(99)), authority.keyStore("second", at(1), at(100)));
        assertRenewKeepsCurrentState(factory, "alias=second");
    }

    /** */
    @Test
    public void testRenewRecoversAfterFailedTrustStoreLoad() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        SSLContext ctx = factory.create();

        factory.keyStore = authority.keyStore("node", at(-1), at(99));
        factory.setTrustManagers((TrustManager[])null);
        factory.setTrustStoreFilePath("missing-trust-store");
        factory.setTrustStorePassword(PASSWORD);

        assertRenewKeepsCurrentState(factory, "missing-trust-store");

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        factory.renew();

        assertNotSame(ctx, factory.create());
        assertEquals(at(84), factory.getRenewalTime());
    }

    /** */
    @Test
    public void testRenewRejectsCertificatesExpiredDuringDateValidation() throws Exception {
        InMemorySslContextFactory factory = dueFactory();
        long expiry = System.currentTimeMillis() / 1_000 * 1_000 + 5_000;
        KeyStore replacement = authority.keyStore("replacement", at(-1), expiry);
        Certificate[] chain = replacement.getCertificateChain("replacement");
        X509Certificate cert = spy((X509Certificate)chain[0]);

        doAnswer(validityCheck -> {
            validityCheck.callRealMethod();

            long deadline = System.currentTimeMillis() + 10_000;

            assertTrue(waitForCondition(() -> System.currentTimeMillis() > expiry, () -> System.currentTimeMillis() < deadline));

            return null;
        }).when(cert).checkValidity(any(Date.class));

        chain[0] = cert;
        replacement.setKeyEntry("replacement", replacement.getKey("replacement", PASSWORD), PASSWORD, chain);
        factory.keyStore = replacement;

        assertRenewKeepsCurrentState(factory, "expired while loading");
    }

    /** @return Factory that has loaded certificates whose renewal window has started. */
    private InMemorySslContextFactory dueFactory() throws Exception {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(authority.keyStore("node", at(-90), at(10)));

        factory.create();

        return factory;
    }

    /** */
    private void assertRenewalWindowStartsNow(KeyStore keys) {
        InMemorySslContextFactory factory = new InMemorySslContextFactory(keys);

        factory.create();

        assertEquals(now, factory.getRenewalTime());
        assertEquals(1, factory.keyStoreLoads);
    }

    /** */
    private static void assertRenewKeepsCurrentState(SslContextFactory factory, String reason) {
        SSLContext ctx = factory.create();
        long renewalTime = factory.getRenewalTime();
        IgniteException err = assertThrows(IgniteException.class, factory::renew);

        assertTrue(err.getCause().getMessage(), err.getCause().getMessage().contains(reason));
        assertSame(ctx, factory.create());
        assertEquals(renewalTime, factory.getRenewalTime());
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
            setRenewalEnabled(true);
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
