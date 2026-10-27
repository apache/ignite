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
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.crypto.KeyGenerator;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.TestCertificateAuthority.certificateChain;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests that a bundle's certificate metadata belongs to the key material used to create its context. */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Store and key password of the generated test material. */
    private static final char[] PASSWORD = TestCertificateAuthority.PWD;

    /** Authority that signs the test certificates. */
    private TestCertificateAuthority ca;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("metadata-ca");
    }

    /** Each bundle loads its store once; fresh bundles do not replace the context cached by the legacy create method. */
    @Test
    public void testFreshBundlePreservesCachedContext() throws Exception {
        KeyStore originalKeys = ca.keyStoreWithPrivateKey("original");
        List<X509Certificate> originalChain = Arrays.asList(certificateChain(originalKeys));
        InMemoryStoreFactory factory = new InMemoryStoreFactory(originalKeys);
        SslBundle original = factory.createSslBundle();

        assertEquals(Map.of("original", originalChain), original.certificateChains());
        assertEquals(1, factory.loadKeyStoreCalls);
        assertEquals(1, factory.parameterChecks);

        SSLContext cached = factory.create();

        assertSame(cached, factory.create());
        assertEquals(2, factory.loadKeyStoreCalls);
        assertEquals(2, factory.parameterChecks);

        KeyStore replacementKeys = ca.keyStoreWithPrivateKey("replacement");
        List<X509Certificate> replacementChain = Arrays.asList(certificateChain(replacementKeys));

        assertNotEquals(originalChain.get(0), replacementChain.get(0));

        factory.store = replacementKeys;

        SslBundle fresh = factory.createSslBundle();

        assertNotSame(cached, fresh.context());
        assertNotSame(original.context(), fresh.context());
        assertEquals(Map.of("replacement", replacementChain), fresh.certificateChains());
        assertEquals(Map.of("original", originalChain), original.certificateChains());
        assertSame(cached, factory.create());
        assertEquals(3, factory.loadKeyStoreCalls);
        assertEquals(3, factory.parameterChecks);
    }

    /** TLS parameters apply to the context while its metadata remains available separately. */
    @Test
    public void testMetadataWithTlsParameters() throws Exception {
        KeyStore store = ca.keyStoreWithPrivateKey("node");
        InMemoryStoreFactory factory = new InMemoryStoreFactory(store);
        String[] protocols = {"TLSv1.2"};
        String[] ciphers = {"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"};

        factory.setProtocols(protocols);
        factory.setCipherSuites(ciphers);

        SslBundle bundle = factory.createSslBundle();

        assertEquals(Map.of("node", Arrays.asList(certificateChain(store))), bundle.certificateChains());

        for (SSLContext ctx : new SSLContext[] {bundle.context(), factory.create()}) {
            SSLEngine engine = ctx.createSSLEngine("peer.example", 10800);

            assertArrayEquals(protocols, engine.getEnabledProtocols());
            assertArrayEquals(ciphers, engine.getEnabledCipherSuites());
            assertEquals("peer.example", engine.getPeerHost());
            assertEquals(10800, engine.getPeerPort());
        }
    }

    /** Every private-key alias retains its own chain. */
    @Test
    public void testMultiplePrivateKeysRetainAllChains() throws Exception {
        String firstKey = "first-key";
        String secondKey = "second-key";
        KeyStore store = ca.keyStoreWithPrivateKey(firstKey);
        KeyStore other = ca.keyStoreWithPrivateKey(secondKey);
        List<X509Certificate> firstChain = Arrays.asList(certificateChain(store));
        List<X509Certificate> secondChain = Arrays.asList(certificateChain(other));

        store.setKeyEntry(secondKey, other.getKey(secondKey, PASSWORD), PASSWORD, other.getCertificateChain(secondKey));

        SslBundle bundle = new InMemoryStoreFactory(store).createSslBundle();

        assertEquals(Map.of(firstKey, firstChain, secondKey, secondChain), bundle.certificateChains());
        assertEquals(List.of(firstKey, secondKey), new ArrayList<>(bundle.certificateChains().keySet()));
    }

    /** Trusted certificates and secret keys do not count as private keys. */
    @Test
    public void testOnlyPrivateKeyEntriesCount() throws Exception {
        String keyAlias = "private-key";
        KeyStore keys = ca.keyStoreWithPrivateKey(keyAlias);

        // JCEKS can hold both private keys and secret keys.
        KeyStore store = KeyStore.getInstance("JCEKS");

        store.load(null, PASSWORD);
        store.setCertificateEntry("trusted", keys.getCertificate(keyAlias));
        store.setEntry("secret", new KeyStore.SecretKeyEntry(KeyGenerator.getInstance("AES").generateKey()),
            new KeyStore.PasswordProtection(PASSWORD));

        InMemoryStoreFactory factory = new InMemoryStoreFactory(store);
        SslBundle withoutKey = factory.createSslBundle();

        assertEquals(Map.of(), withoutKey.certificateChains());
        assertNotNull(withoutKey.context().getSocketFactory());

        store.setKeyEntry(keyAlias, keys.getKey(keyAlias, PASSWORD), PASSWORD, keys.getCertificateChain(keyAlias));

        SslBundle withKey = factory.createSslBundle();

        assertEquals(Map.of(keyAlias, Arrays.asList(certificateChain(keys))), withKey.certificateChains());
        assertEquals("The earlier bundle remains keyless", Map.of(), withoutKey.certificateChains());
    }

    /** A private key with a non-X.509 chain is retained as an alias with unknown certificate metadata. */
    @Test
    public void testNonX509ChainRetainsPrivateKeyAlias() throws Exception {
        String alias = "private-key";
        KeyStore store = ca.keyStoreWithPrivateKey(alias);
        X509Certificate cert = (X509Certificate)store.getCertificate(alias);
        Certificate otherType = new Certificate("test-certificate") {
            /** {@inheritDoc} */
            @Override public byte[] getEncoded() throws CertificateEncodingException {
                return cert.getEncoded();
            }

            /** {@inheritDoc} */
            @Override public void verify(PublicKey key) {
                throw new UnsupportedOperationException();
            }

            /** {@inheritDoc} */
            @Override public void verify(PublicKey key, String provider) {
                throw new UnsupportedOperationException();
            }

            /** {@inheritDoc} */
            @Override public PublicKey getPublicKey() {
                return cert.getPublicKey();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "Test certificate with a non-X.509 type";
            }
        };

        store.setKeyEntry(alias, store.getKey(alias, PASSWORD), PASSWORD, new Certificate[] {otherType});

        KeyMaterial material = new KeyMaterial(store, PASSWORD, SslContextFactory.DFLT_KEY_ALGORITHM);

        assertEquals(Map.of(alias, List.of()), material.certificateChains());
    }

    /** Legacy hooks run once per new context and leave metadata unknown rather than reporting no private keys. */
    @Test
    public void testLegacyHooksRunOncePerBuiltContext() {
        LegacyFactory factory = new LegacyFactory();
        SSLContext first = factory.create();

        assertSame(first, factory.create());
        assertEquals(1, factory.parameterChecks);
        assertEquals(1, factory.keyManagerCalls);
        assertEquals(1, factory.trustManagerCalls);

        SslBundle next = factory.createSslBundle();

        assertNotSame(first, next.context());
        assertNull(next.certificateChains());
        assertSame(first, factory.create());
        assertEquals(2, factory.parameterChecks);
        assertEquals(2, factory.keyManagerCalls);
        assertEquals(2, factory.trustManagerCalls);
    }

    /** Existing subclasses can still populate the protected cache before the first create call. */
    @Test
    public void testLegacyProtectedCache() throws Exception {
        LegacyFactory factory = new LegacyFactory();
        SSLContext prepared = TestCertificateAuthority.context(ca.keyStoreWithPrivateKey(), ca.trustStore());

        factory.sslCtx.set(prepared);

        assertSame(prepared, factory.create());
        assertEquals(0, factory.parameterChecks);
        assertEquals(0, factory.keyManagerCalls);
        assertEquals(0, factory.trustManagerCalls);

        assertNotSame(prepared, factory.createSslBundle().context());
        assertSame(prepared, factory.create());
    }

    /** A subclass can still obtain key managers from the file factory's legacy hook. */
    @Test
    public void testLegacyKeyManagerAdapter() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(ca.keyStoreWithPrivateKey());
        KeyManager[] mgrs = factory.legacyKeyManagers();

        assertNotNull(mgrs);
        assertTrue(mgrs.length > 0);
        assertEquals(1, factory.loadKeyStoreCalls);
    }

    /** Invalid parameters are reported before loading the store. */
    @Test
    public void testInvalidParametersDoNotLoadStore() throws Exception {
        InMemoryStoreFactory factory = new InMemoryStoreFactory(ca.keyStoreWithPrivateKey());

        factory.setTrustManagers((TrustManager[])null);

        GridTestUtils.assertThrows(log, (Callable<SslBundle>)factory::createSslBundle,
            IgniteException.class, "trustManagers must be provided");

        assertEquals(1, factory.parameterChecks);
        assertEquals(0, factory.loadKeyStoreCalls);
    }

    /** A factory implementing neither a bundle builder nor the legacy key-manager hook reports the missing implementation. */
    @Test
    public void testMissingKeyMaterialImplementation() {
        AbstractSslContextFactory factory = new AbstractSslContextFactory() {
            /** {@inheritDoc} */
            @Override protected void checkParameters() {
                // No parameters are required by this factory.
            }

            /** {@inheritDoc} */
            @Override protected TrustManager[] createTrustManagers() {
                return null;
            }
        };

        String msg = "Subclass must override createSslBundle() or createKeyManagers()";

        Throwable err = GridTestUtils.assertThrows(log, factory::create, IgniteException.class, msg);

        assertTrue(err.getCause() instanceof SSLException);

        err = GridTestUtils.assertThrows(log, (Callable<SslBundle>)factory::createSslBundle, IgniteException.class, msg);

        assertTrue(err.getCause() instanceof SSLException);
    }

    /** A custom store source does not need the legacy key-manager hook and keeps its own copy of the key password. */
    @Test
    public void testLoadedMaterialBypassesLegacyKeyManagerHook() throws Exception {
        KeyStore store = ca.keyStoreWithPrivateKey("node");
        char[] pwd = PASSWORD.clone();
        KeyMaterial material = new KeyMaterial(store, pwd, SslContextFactory.DFLT_KEY_ALGORITHM);

        Arrays.fill(pwd, '?');

        AbstractSslContextFactory factory = new AbstractSslContextFactory() {
            /** {@inheritDoc} */
            @Override public SslBundle createSslBundle() {
                try {
                    checkParameters();

                    return createSslBundle(material);
                }
                catch (SSLException e) {
                    throw new IgniteException(e);
                }
            }

            /** {@inheritDoc} */
            @Override protected void checkParameters() {
                // The test supplies already loaded material.
            }

            /** {@inheritDoc} */
            @Override protected TrustManager[] createTrustManagers() {
                return null;
            }
        };

        assertEquals(Map.of("node", Arrays.asList(certificateChain(store))), factory.createSslBundle().certificateChains());
    }

    /** Mutating the source map, its lists or the store cannot change an existing bundle. */
    @Test
    public void testImmutableCertificateMetadata() throws Exception {
        KeyStore store = ca.keyStoreWithPrivateKey("node");
        SslBundle loaded = new InMemoryStoreFactory(store).createSslBundle();
        List<X509Certificate> expected = Arrays.asList(certificateChain(store));
        List<X509Certificate> inputChain = new ArrayList<>(expected);
        Map<String, List<X509Certificate>> input = new HashMap<>();

        input.put("node", inputChain);

        SslBundle bundle = new SslBundle(loaded.context(), input);

        inputChain.clear();
        input.clear();
        store.deleteEntry("node");

        assertEquals(Map.of("node", expected), loaded.certificateChains());
        assertEquals(Map.of("node", expected), bundle.certificateChains());
        assertSame(loaded.context(), bundle.context());

        GridTestUtils.assertThrows(log, () -> bundle.certificateChains().put("other", List.of()),
            UnsupportedOperationException.class, null);
        GridTestUtils.assertThrows(log, () -> bundle.certificateChains().get("node").set(0, expected.get(1)),
            UnsupportedOperationException.class, null);
    }

    /** A file factory whose existing store-loading hook supplies the test material. */
    private static class InMemoryStoreFactory extends SslContextFactory {
        /** */
        private KeyStore store;

        /** */
        private int loadKeyStoreCalls;

        /** */
        private int parameterChecks;

        /** */
        InMemoryStoreFactory(KeyStore store) {
            this.store = store;

            setKeyStoreType(store.getType());
            setKeyStoreFilePath("test-store");
            setKeyStorePassword(PASSWORD);
            setTrustManagers(getDisabledTrustManager());
        }

        /** {@inheritDoc} */
        @Override protected void checkParameters() throws SSLException {
            parameterChecks++;

            super.checkParameters();
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) {
            loadKeyStoreCalls++;

            return store;
        }

        /** @return Key managers supplied by the inherited legacy hook. */
        @SuppressWarnings("deprecation")
        private KeyManager[] legacyKeyManagers() throws SSLException {
            return super.createKeyManagers();
        }
    }

    /** A subclass that implements only the legacy manager hooks. */
    private static class LegacyFactory extends AbstractSslContextFactory {
        /** */
        private int parameterChecks;

        /** */
        private int keyManagerCalls;

        /** */
        private int trustManagerCalls;

        /** {@inheritDoc} */
        @Override protected void checkParameters() {
            parameterChecks++;
        }

        /** {@inheritDoc} */
        @Override protected KeyManager[] createKeyManagers() {
            keyManagerCalls++;

            return null;
        }

        /** {@inheritDoc} */
        @Override protected TrustManager[] createTrustManagers() {
            trustManagerCalls++;

            return null;
        }
    }
}
