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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.crypto.KeyGenerator;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/** Tests how the factory loads, publishes and retains its context and certificate metadata. */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Each accepted context and its certificate chains come from the same store read. */
    @Test
    public void testInitPublishesContextAndChainsFromOneKeyStoreRead() throws Exception {
        KeyStore first = nodeStore("node01");
        KeyStore second = nodeStore("node02");
        InMemoryStoreFactory factory = new InMemoryStoreFactory(first);

        factory.init();

        SSLContext original = factory.create();
        Map<String, List<X509Certificate>> originalChains = factory.certificateChains();

        factory.store = second;

        factory.init();

        assertEquals(2, factory.loads);
        assertEquals(Map.of("node01", chain(first, "node01")), originalChains);
        assertEquals(Map.of("node02", chain(second, "node02")), factory.certificateChains());
        assertNotSame(original, factory.create());
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

    /** During a reload, readers get the accepted context and chains while another reload waits for its turn. */
    @Test
    public void testReadersProceedWhileReloadsRunInOrder() throws Exception {
        InMemoryStoreFactory factory = spy(new InMemoryStoreFactory(nodeStore("node01")));
        SSLContext accepted = factory.create();
        Map<String, List<X509Certificate>> acceptedChains = factory.certificateChains();
        KeyStore replacement = nodeStore("node02");
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        long timeout = 10_000;

        doAnswer(inv -> {
            loading.countDown();
            release.await();

            return inv.callRealMethod();
        }).doReturn(replacement).when(factory).loadKeyStore(anyString(), anyString(), any(char[].class));

        IgniteInternalFuture<Void> first = GridTestUtils.runAsync(() -> factory.init());
        FutureTask<Void> second = new FutureTask<>(factory::init, null);
        Thread secondThread = new Thread(second, "second-init");

        try {
            assertTrue("Replacement load started", loading.await(timeout, TimeUnit.MILLISECONDS));
            assertSame(accepted, GridTestUtils.runAsync(() -> factory.create()).get(timeout));
            assertEquals(acceptedChains, GridTestUtils.runAsync(() -> factory.certificateChains()).get(timeout));

            secondThread.start();

            assertTrue(GridTestUtils.waitForCondition(() -> secondThread.getState() == Thread.State.BLOCKED || second.isDone(), timeout));
            assertFalse("The second reload must wait for the first", second.isDone());
        }
        finally {
            release.countDown();
            first.get(timeout);
        }

        second.get(timeout, TimeUnit.MILLISECONDS);

        assertNotSame(accepted, factory.create());
        assertEquals(Map.of("node02", chain(replacement, "node02")), factory.certificateChains());
    }

    /** Invalid settings can be corrected after first use fails. A later failure preserves the accepted context and chains. */
    @Test
    public void testFailedInitKeepsAcceptedContextAndChains() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(nodeStore("node01"));
        String invalidProtocol = "unsupported-protocol";

        factory.setProtocols(invalidProtocol);

        GridTestUtils.assertThrows(log, factory::create, IllegalArgumentException.class, invalidProtocol);

        factory.setProtocols((String[])null);

        SSLContext accepted = factory.create();
        Map<String, List<X509Certificate>> chains = factory.certificateChains();

        factory.loadFailure = "Key store is unavailable";

        IgniteException unavailable = assertThrows(IgniteException.class, factory::init);

        GridTestUtils.assertContains(log, unavailable.getMessage(), factory.loadFailure);
        assertSame(accepted, factory.create());
        assertEquals(chains, factory.certificateChains());

        factory.loadFailure = null;
        factory.setProtocols(invalidProtocol);

        IllegalArgumentException badProtocol = assertThrows(IllegalArgumentException.class, factory::init);

        GridTestUtils.assertContains(log, badProtocol.getMessage(), invalidProtocol);
        assertSame(accepted, factory.create());
        assertEquals(chains, factory.certificateChains());
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
        SSLContext original = factory.create();
        SslContextFactory restored = TEST_JDK_MARSHALLER.unmarshal(TEST_JDK_MARSHALLER.marshal(factory),
            getClass().getClassLoader());

        assertNotSame(original, restored.create());
        assertEquals(factory.certificateChains(), restored.certificateChains());
    }

    /** A client factory can return no key store when it authenticates only the server. */
    @Test
    public void testClientWithoutKeyStore() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(null);
        factory.init();

        assertEquals(Map.of(), factory.certificateChains());
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
        factory.init();

        assertEquals(Map.of(firstAlias, chain(keys, firstAlias), secondAlias, chain(otherKeys, secondAlias)),
            factory.certificateChains());
    }

    /** An empty key store yields a context without private keys and an empty map. */
    @Test
    public void testEmptyStoreCreatesKeylessContext() throws Exception {
        KeyStore empty = KeyStore.getInstance("JKS");

        empty.load(null, null);

        InMemoryStoreFactory factory = new InMemoryStoreFactory(empty);

        factory.init();

        assertEquals(Map.of(), factory.certificateChains());
    }

    /** The factory reports chains in alias order and does not expose mutable metadata. */
    @Test
    public void testChainsAreSortedAndImmutable() throws Exception {
        KeyStore store = nodeStore("node02");
        KeyStore other = nodeStore("node01");
        KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(keyStorePassword().toCharArray());

        store.setEntry("node01", other.getEntry("node01", protection), protection);

        List<X509Certificate> first = new ArrayList<>(chain(other, "node01"));
        List<X509Certificate> second = chain(store, "node02");
        Map<String, List<X509Certificate>> reported = new LinkedHashMap<>();

        reported.put("node02", second);
        reported.put("node01", first);

        SslContextFactory factory = reportingFactory(new InMemoryStoreFactory(store).create(), reported);

        factory.init();
        reported.clear();
        first.clear();

        Map<String, List<X509Certificate>> chains = factory.certificateChains();

        assertEquals(List.of("node01", "node02"), new ArrayList<>(chains.keySet()));
        assertEquals(Map.of("node01", chain(other, "node01"), "node02", second), chains);
        assertThrows(UnsupportedOperationException.class, () -> chains.put("c", List.of()));
        assertThrows(UnsupportedOperationException.class, () -> chains.get("node01").clear());
    }

    /** A custom source cannot report a key alias with an empty chain. */
    @Test
    public void testFactoryRejectsEmptyReportedChain() throws Exception {
        String alias = "missing-chain";
        SslContextFactory factory = reportingFactory(SSLContext.getDefault(), Map.of(alias, List.of()));

        IllegalArgumentException err = assertThrows(IllegalArgumentException.class, factory::init);

        assertTrue(err.getMessage().contains(alias));
    }

    /** Unavailable metadata stays {@code null}, so it is not mistaken for a store without certificate chains. */
    @Test
    public void testFactoryKeepsUnavailableMetadata() throws Exception {
        SslContextFactory factory = reportingFactory(SSLContext.getDefault(), null);

        factory.init();

        assertNull(factory.certificateChains());
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

        GridTestUtils.assertContains(log, err.getMessage(), "keyStoreType=" + store.getType());
        assertTrue(X.hasCause(err, UnrecoverableKeyException.class));
    }

    /** A factory that supplies an initialized context and reports its certificate chains explicitly. */
    private static SslContextFactory reportingFactory(SSLContext ctx, Map<String, List<X509Certificate>> chains) {
        return new SslContextFactory() {
            /** {@inheritDoc} */
            @Override protected SSLContext createSslContext(Consumer<Map<String, List<X509Certificate>>> reportChains) {
                reportChains.accept(chains);

                return ctx;
            }
        };
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
     * Reads the key store from memory and counts the reads. A trust manager is supplied directly.
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
