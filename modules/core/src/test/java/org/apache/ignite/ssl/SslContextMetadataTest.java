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
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.KeyGenerator;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.TestCertificateAuthority.PWD;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.keyStoreWithoutCertificateChain;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Factory initialization publishes a context and metadata from one store read; failed initialization preserves the previous
 * bundle. Reading the accepted bundle or context does not reload stores. {@link KeyMaterial} reports nonempty X.509 chains
 * without recovering keys and keeps its own key password. {@link SslBundle} keeps sorted immutable metadata, or {@code null}.
 */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Authority that signs the test certificates. */
    private TestCertificateAuthority ca;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("metadata-ca");
    }

    /** Each initialization publishes a fresh context with metadata from the same, single key store read. */
    @Test
    public void testInitPublishesBundleFromOneKeyStoreRead() throws Exception {
        KeyStore first = ca.keyStoreWithPrivateKey("first");
        InMemoryStoreFactory factory = new InMemoryStoreFactory(first);

        SslBundle firstBundle = factory.init();

        assertEquals(1, factory.loads);
        assertSame(firstBundle, factory.sslBundle());
        assertEquals(Map.of("first", chain(first)), firstBundle.certificateChains());

        assertSame(firstBundle.context(), factory.create());

        KeyStore second = ca.keyStoreWithPrivateKey("second");

        factory.store = second;

        SslBundle secondBundle = factory.init();

        assertEquals(2, factory.loads);
        assertSame(secondBundle, factory.sslBundle());
        assertEquals(Map.of("second", chain(second)), secondBundle.certificateChains());
        assertEquals(Map.of("first", chain(first)), firstBundle.certificateChains());
        assertNotSame(firstBundle.context(), secondBundle.context());
        assertSame(secondBundle.context(), factory.create());
    }

    /** Concurrent first calls initialize once; obtaining the context and accepted snapshot never reloads the store. */
    @Test
    public void testCreateAndSnapshotDoNotReloadStore() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(ca.keyStoreWithPrivateKey());
        int threadCnt = 4;
        CyclicBarrier start = new CyclicBarrier(threadCnt);
        AtomicReference<SSLContext> sharedRef = new AtomicReference<>();

        GridTestUtils.runMultiThreaded(() -> {
            start.await();

            SSLContext ctx = factory.create();

            sharedRef.compareAndSet(null, ctx);
            assertSame(sharedRef.get(), ctx);

            return null;
        }, threadCnt, "ssl-context-create");

        SslBundle accepted = factory.sslBundle();
        SSLContext shared = sharedRef.get();

        factory.loadFailure = "Store must not be read again";

        assertSame(shared, factory.create());
        assertSame(accepted, factory.sslBundle());
        assertEquals(1, factory.loads);
    }

    /** Readers retain the accepted material while initialization fails; a later attempt can replace it. */
    @Test
    public void testFailedInitRetainsAcceptedBundle() throws Exception {
        InMemoryStoreFactory factory = spy(new InMemoryStoreFactory(ca.keyStoreWithPrivateKey()));
        SslBundle accepted = factory.init();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        long timeout = 10_000;

        factory.loadFailure = "Key store is unavailable";

        doAnswer(inv -> {
            loading.countDown();
            release.await();

            return inv.callRealMethod();
        }).doCallRealMethod().when(factory).createSslBundle();

        IgniteInternalFuture<SslBundle> initialization = GridTestUtils.runAsync(() -> {
            return factory.init();
        });
        IgniteInternalFuture<SslBundle> reading = null;

        try {
            assertTrue(loading.await(timeout, TimeUnit.MILLISECONDS));

            reading = GridTestUtils.runAsync(() -> {
                assertSame(accepted.context(), factory.create());

                return factory.sslBundle();
            });

            assertSame(accepted, reading.get(timeout));
        }
        finally {
            release.countDown();

            if (reading != null)
                reading.cancel();

            GridTestUtils.assertThrowsWithCause(() -> initialization.get(timeout), SSLException.class);
        }

        assertSame(accepted, factory.sslBundle());
        assertSame(accepted.context(), factory.create());
        assertNotNull(accepted.context().createSSLEngine());
        assertEquals(2, factory.loads);

        factory.loadFailure = null;

        SslBundle replacement = factory.init();

        assertNotSame(accepted, replacement);
        assertSame(replacement, factory.sslBundle());
        assertSame(replacement.context(), factory.create());
        assertEquals(3, factory.loads);
    }

    /** A failed first initialization leaves the factory able to initialize on the next context request. */
    @Test
    public void testFirstInitializationFailureCanBeRetried() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(ca.keyStoreWithPrivateKey());

        factory.loadFailure = "Key store is unavailable";

        GridTestUtils.assertThrows(log, factory::create, IgniteException.class, factory.loadFailure);

        factory.loadFailure = null;

        SSLContext shared = factory.create();

        assertSame(shared, factory.create());
        assertNotNull(factory.sslBundle().context().createSSLEngine());
        assertEquals(2, factory.loads);
    }

    /** Unsupported TLS parameters are rejected before a replacement becomes visible. */
    @Test
    public void testInvalidTlsParametersDoNotReplaceAcceptedBundle() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(ca.keyStoreWithPrivateKey());
        SslBundle accepted = factory.init();
        SSLContext shared = factory.create();

        factory.setProtocols("unsupported-protocol");

        GridTestUtils.assertThrows(log, factory::init, IllegalArgumentException.class, null);

        assertSame(accepted, factory.sslBundle());
        assertSame(shared, factory.create());
        assertNotNull(shared.createSSLEngine());

        factory.setProtocols((String[])null);
        factory.setCipherSuites("unsupported-cipher-suite");

        GridTestUtils.assertThrows(log, factory::init, IllegalArgumentException.class, null);

        assertSame(accepted, factory.sslBundle());
        assertSame(shared, factory.create());
        assertNotNull(shared.createSSLEngine());
        assertEquals(3, factory.loads);
    }

    /** Missing parameters are reported before the key store is read. */
    @Test
    public void testInvalidParametersDoNotLoadStore() throws Exception {
        KeyStore empty = KeyStore.getInstance("JKS");

        empty.load(null, null);

        InMemoryStoreFactory factory = new InMemoryStoreFactory(empty);

        factory.setTrustManagers((TrustManager[])null);

        GridTestUtils.assertThrows(log, factory::create, IgniteException.class,
            "either trustStoreFilePath or trustManagers must be provided");
        assertEquals(0, factory.loads);
    }

    /** Metadata includes only nonempty X.509 chains of key entries, without recovering the keys. */
    @Test
    public void testOnlyKeyEntriesWithX509Chains() throws Exception {
        String firstAlias = "first-key";
        String secondAlias = "second-key";
        KeyStore first = ca.keyStoreWithPrivateKey(firstAlias);
        KeyStore second = ca.keyStoreWithPrivateKey(secondAlias);
        KeyStore.PasswordProtection prot = new KeyStore.PasswordProtection(PWD);

        // JCEKS can hold both private keys and secret keys.
        KeyStore store = keyStoreWithoutCertificateChain("JCEKS", "key-without-chain");

        store.setEntry(firstAlias, first.getEntry(firstAlias, prot), prot);
        store.setEntry(secondAlias, second.getEntry(secondAlias, prot), prot);
        store.setCertificateEntry("trusted", first.getCertificate(firstAlias));
        store.setEntry("secret", new KeyStore.SecretKeyEntry(KeyGenerator.getInstance("AES").generateKey()),
            new KeyStore.PasswordProtection("unrelated-password".toCharArray()));
        store.setKeyEntry("non-x509-chain", first.getKey(firstAlias, PWD), PWD,
            new Certificate[] {first.getCertificate(firstAlias), mock(Certificate.class)});

        KeyMaterial material = new KeyMaterial(store, PWD, SslContextFactory.DFLT_KEY_ALGORITHM);

        assertEquals(Map.of(firstAlias, chain(first), secondAlias, chain(second)), material.certificateChains());
    }

    /** An empty key store yields a context without private keys and an empty map. */
    @Test
    public void testEmptyStoreCreatesKeylessContext() throws Exception {
        KeyStore empty = KeyStore.getInstance("JKS");

        empty.load(null, null);

        SslBundle bundle = new InMemoryStoreFactory(empty).init();

        assertEquals(Map.of(), bundle.certificateChains());
    }

    /** Material keeps its own copy of the key password, so clearing the caller's array does not lock the keys. */
    @Test
    public void testLoadedMaterialCopiesKeyPassword() throws Exception {
        char[] pwd = PWD.clone();
        KeyMaterial material = new KeyMaterial(ca.keyStoreWithPrivateKey("node"), pwd, SslContextFactory.DFLT_KEY_ALGORITHM);

        Arrays.fill(pwd, '?');

        X509KeyManager mgr = (X509KeyManager)material.keyManagers()[0];

        assertNotNull(mgr.getPrivateKey("node"));
    }

    /** A bundle orders aliases and copies the map and its lists, so later changes to the input do not reach it. */
    @Test
    public void testBundleKeepsSortedImmutableCopy() throws Exception {
        X509Certificate aCert = mock(X509Certificate.class);
        X509Certificate bCert = mock(X509Certificate.class);
        List<X509Certificate> aChain = new ArrayList<>(List.of(aCert));
        Map<String, List<X509Certificate>> input = new LinkedHashMap<>();

        input.put("b", List.of(bCert));
        input.put("a", aChain);

        SslBundle bundle = new SslBundle(SSLContext.getDefault(), input);

        aChain.clear();
        input.clear();

        Map<String, List<X509Certificate>> chains = bundle.certificateChains();

        assertEquals(List.of("a", "b"), new ArrayList<>(chains.keySet()));
        assertEquals(Map.of("a", List.of(aCert), "b", List.of(bCert)), chains);
        GridTestUtils.assertThrows(log, () -> chains.put("c", List.of()), UnsupportedOperationException.class, null);
        GridTestUtils.assertThrows(log, () -> chains.get("a").set(0, bCert), UnsupportedOperationException.class, null);
    }

    /** Custom factories must omit entries without a chain instead of reporting an empty chain. */
    @Test
    public void testBundleRejectsEmptyChain() throws Exception {
        String alias = "missing-chain";

        GridTestUtils.assertThrows(log, () -> new SslBundle(SSLContext.getDefault(), Map.of(alias, List.of())),
            IllegalArgumentException.class, "Certificate chain must not be empty [alias=" + alias + ']');
    }

    /** Unknown metadata stays {@code null}, so it is not mistaken for a store without certificate chains. */
    @Test
    public void testBundleKeepsUnknownMetadata() throws Exception {
        assertNull(new SslBundle(SSLContext.getDefault(), null).certificateChains());
    }

    /** A key the store password cannot open is reported with the store type and path and keeps the original cause. */
    @Test
    public void testKeyPasswordFailureNamesStore() throws Exception {
        KeyStore store = ca.keyStoreWithPrivateKey("private-key");

        store.setKeyEntry("private-key", store.getKey("private-key", PWD), "other-key-password".toCharArray(),
            store.getCertificateChain("private-key"));

        InMemoryStoreFactory factory = new InMemoryStoreFactory(store);

        String storePath = "unreadable-key.jks";

        factory.setKeyStoreFilePath(storePath);

        String storeDetails = "keyStoreFile=" + storePath;
        Throwable err = GridTestUtils.assertThrows(log, factory::create, IgniteException.class, storeDetails);

        assertTrue(err.getCause() instanceof SSLException);
        assertTrue(err.getMessage().contains("keyStoreType=" + store.getType()));
        assertTrue(err.getCause().getCause() instanceof UnrecoverableKeyException);
    }

    /**
     * File factory that reads its key store from memory and counts the reads. Trust is disabled, so the trust store is never
     * read and every read is the key store's.
     */
    private static class InMemoryStoreFactory extends SslContextFactory {
        /** Store returned by the next read. */
        private KeyStore store;

        /** Number of key store reads. */
        private int loads;

        /** Message of the read failure, or {@code null} while the store is readable. */
        private String loadFailure;

        /** @param store Store returned by reads. */
        InMemoryStoreFactory(KeyStore store) {
            this.store = store;

            setKeyStoreType(store.getType());
            setKeyStoreFilePath("in-memory");
            setKeyStorePassword(PWD);
            setTrustManagers(getDisabledTrustManager());
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) throws SSLException {
            loads++;

            if (loadFailure != null)
                throw new SSLException(loadFailure);

            return store;
        }
    }
}
