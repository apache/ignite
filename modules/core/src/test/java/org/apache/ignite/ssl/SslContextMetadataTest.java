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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.security.KeyStore;
import java.security.KeyStoreSpi;
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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/** Tests how the factory loads, publishes and retains its context and certificate metadata. */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Each accepted context and its certificate chains come from the same store read. */
    @Test
    public void testInitPublishesBundleFromOneKeyStoreRead() throws Exception {
        KeyStore first = nodeStore("node01");
        KeyStore second = nodeStore("node02");
        InMemoryStoreFactory factory = new InMemoryStoreFactory(first);

        SslBundle original = factory.init();

        factory.store = second;

        SslBundle replacement = factory.init();

        assertEquals(2, factory.loads);
        assertEquals(Map.of("node01", chain(first, "node01")), original.certificateChains());
        assertEquals(Map.of("node02", chain(second, "node02")), replacement.certificateChains());
        assertNotSame(original.context(), replacement.context());
        assertSame(replacement.context(), factory.create());
    }

    /** Concurrent requests share the context produced by the first store read. */
    @Test
    public void testConcurrentFirstCreateLoadsOnce() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(nodeStore("node01"));
        int threads = 4;
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicReference<SSLContext> shared = new AtomicReference<>();

        GridTestUtils.runMultiThreaded(() -> {
            start.await();

            SSLContext ctx = factory.create();

            shared.compareAndSet(null, ctx);
            assertSame(shared.get(), ctx);

            return null;
        }, threads, "ssl-create");

        assertEquals(1, factory.loads);
    }

    /** A slow store read does not block a caller that can already use the accepted context. */
    @Test
    public void testCreateDoesNotWaitForInit() throws Exception {
        InMemoryStoreFactory factory = spy(new InMemoryStoreFactory(nodeStore("node01")));
        SSLContext accepted = factory.create();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        long timeout = 10_000;

        doAnswer(inv -> {
            loading.countDown();
            release.await();

            return inv.callRealMethod();
        }).when(factory).createSslBundle();

        IgniteInternalFuture<SslBundle> init = GridTestUtils.runAsync(() -> factory.init());

        try {
            assertTrue("Replacement load started", loading.await(timeout, TimeUnit.MILLISECONDS));
            assertSame(accepted, GridTestUtils.runAsync(() -> factory.create()).get(timeout));
        }
        finally {
            release.countDown();
            init.get(timeout);
        }
    }

    /** Invalid settings can be corrected after first use fails. A later failure leaves the last accepted bundle usable. */
    @Test
    public void testFailedInitKeepsAcceptedBundle() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(nodeStore("node01"));
        String invalidProtocol = "unsupported-protocol";

        factory.setProtocols(invalidProtocol);

        GridTestUtils.assertThrows(log, factory::create, IllegalArgumentException.class, invalidProtocol);

        factory.setProtocols((String[])null);

        SslBundle accepted = factory.sslBundle();

        factory.loadFailure = "Key store is unavailable";

        GridTestUtils.assertThrows(log, factory::init, IgniteException.class, factory.loadFailure);
        assertSame(accepted, factory.sslBundle());

        factory.loadFailure = null;
        factory.setProtocols(invalidProtocol);

        GridTestUtils.assertThrows(log, factory::init, IllegalArgumentException.class, invalidProtocol);
        assertSame(accepted, factory.sslBundle());
    }

    /** A context already handed to a consumer retains its TLS settings after the factory reloads. */
    @Test
    public void testPreviousContextKeepsItsSettings() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(nodeStore("node01"));
        String[] originalProtocols = {"TLSv1.2"};
        String[] replacementProtocols = {"TLSv1.3"};

        factory.setProtocols(originalProtocols);

        SSLContext original = factory.create();

        factory.setProtocols(replacementProtocols);
        factory.init();

        assertArrayEquals(originalProtocols, original.createSSLEngine().getEnabledProtocols());
        assertArrayEquals(replacementProtocols, factory.create().createSSLEngine().getEnabledProtocols());
    }

    /** Deserializing a configured factory loads fresh material at first use. */
    @Test
    public void testSerializationKeepsConfiguration() throws Exception {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");
        SslBundle original = factory.init();
        SslContextFactory restored = TEST_JDK_MARSHALLER.unmarshal(TEST_JDK_MARSHALLER.marshal(factory),
            getClass().getClassLoader());

        assertEquals(factory.getKeyStoreFilePath(), restored.getKeyStoreFilePath());
        assertNotSame(original.context(), restored.create());
        assertEquals(original.certificateChains(), restored.sslBundle().certificateChains());
    }

    /** A client factory can return no key store when it authenticates only the server. */
    @Test
    public void testClientWithoutKeyStore() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(null);
        SslBundle client = factory.init();

        assertEquals(Map.of(), client.certificateChains());
        assertNotNull(client.context().createSSLEngine());
    }

    /** DTLS supports engines but cannot supply the TLS sockets used by discovery. */
    @Test
    public void testDatagramContextRejectedAtStartupAndReload() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(nodeStore("node01"));
        String datagramProtocol = "DTLS";

        factory.setProtocol(datagramProtocol);

        GridTestUtils.assertThrows(log, factory::init, UnsupportedOperationException.class, null);

        factory.setProtocol("TLS");

        SslBundle accepted = factory.init();

        factory.setProtocol(datagramProtocol);

        GridTestUtils.assertThrows(log, factory::init, UnsupportedOperationException.class, null);
        assertSame(accepted, factory.sslBundle());
    }

    /** Missing parameters are reported before the key store is read. */
    @Test
    public void testInvalidParametersDoNotLoadStore() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(null);

        factory.setTrustManagers((TrustManager[])null);

        GridTestUtils.assertThrows(log, factory::create, IgniteException.class,
            "either trustStoreFilePath or trustManagers must be provided");
        assertEquals(0, factory.loads);
    }

    /** Metadata includes only nonempty X.509 chains of key entries. */
    @Test
    public void testOnlyKeyEntriesWithX509Chains() throws Exception {
        KeyStore keys = nodeStore("node01");
        KeyStore otherKeys = nodeStore("node02");
        String firstAlias = "node01";
        String secondAlias = "node02";
        char[] pwd = keyStorePassword().toCharArray();
        KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(pwd);

        // The provider can store a key without a chain, although KeyStore.setKeyEntry rejects it.
        KeyStore store = KeyStore.getInstance("JCEKS");
        KeyStoreSpi spi = (KeyStoreSpi)store.getProvider().getService("KeyStore", store.getType()).newInstance(null);

        spi.engineLoad(null, pwd);
        spi.engineSetKeyEntry("key-without-chain", keys.getKey(firstAlias, pwd), pwd, null);

        ByteArrayOutputStream encoded = new ByteArrayOutputStream();

        spi.engineStore(encoded, pwd);
        store.load(new ByteArrayInputStream(encoded.toByteArray()), pwd);
        store.setEntry(firstAlias, keys.getEntry(firstAlias, protection), protection);
        store.setEntry(secondAlias, otherKeys.getEntry(secondAlias, protection), protection);
        store.setCertificateEntry("trusted", keys.getCertificate(firstAlias));
        store.setEntry("secret", new KeyStore.SecretKeyEntry(KeyGenerator.getInstance("AES").generateKey()), protection);
        store.setKeyEntry("mixed-chain", keys.getKey(firstAlias, pwd), pwd,
            new Certificate[] {keys.getCertificate(firstAlias), mock(Certificate.class)});

        // SunX509 requires X.509 entries at initialization. PKIX defers key selection until a handshake.
        InMemoryStoreFactory factory = new InMemoryStoreFactory(store);

        factory.setKeyAlgorithm("PKIX");

        assertEquals(Map.of(firstAlias, chain(keys, firstAlias), secondAlias, chain(otherKeys, secondAlias)),
            factory.init().certificateChains());
    }

    /** An empty key store yields a context without private keys and an empty map. */
    @Test
    public void testEmptyStoreCreatesKeylessContext() throws Exception {
        KeyStore empty = KeyStore.getInstance("JKS");

        empty.load(null, null);

        SslBundle bundle = new InMemoryStoreFactory(empty).init();

        assertEquals(Map.of(), bundle.certificateChains());
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

    /** Unavailable metadata stays {@code null}, so it is not mistaken for a store without certificate chains. */
    @Test
    public void testBundleKeepsUnavailableMetadata() throws Exception {
        assertNull(new SslBundle(SSLContext.getDefault(), null).certificateChains());
    }

    /** A key the store password cannot open is reported with the store type and path and keeps the original cause. */
    @Test
    public void testKeyPasswordFailureNamesStore() throws Exception {
        String alias = "node01";
        KeyStore store = nodeStore(alias);

        store.setKeyEntry(alias, store.getKey(alias, keyStorePassword().toCharArray()), "other-key-password".toCharArray(),
            store.getCertificateChain(alias));

        InMemoryStoreFactory factory = new InMemoryStoreFactory(store);

        String storePath = "unreadable-key.jks";

        factory.setKeyStoreFilePath(storePath);

        String storeDetails = "keyStoreFile=" + storePath;
        Throwable err = GridTestUtils.assertThrows(log, factory::create, IgniteException.class, storeDetails);

        assertTrue(err.getCause() instanceof SSLException);
        assertTrue(err.getMessage().contains("keyStoreType=" + store.getType()));
        assertTrue(err.getCause().getCause() instanceof UnrecoverableKeyException);
    }

    /** Loads one of the node key stores shipped with the test suite. */
    private static KeyStore nodeStore(String name) throws Exception {
        return KeyStore.getInstance(new File(keyStorePath(name)), keyStorePassword().toCharArray());
    }

    /** Returns the certificate chain saved in the source store. */
    private static List<X509Certificate> chain(KeyStore store, String alias) throws Exception {
        Certificate[] chain = store.getCertificateChain(alias);

        return List.of(Arrays.copyOf(chain, chain.length, X509Certificate[].class));
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

            if (store != null)
                setKeyStoreType(store.getType());

            setKeyStoreFilePath("in-memory");
            setKeyStorePassword(keyStorePassword().toCharArray());
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
