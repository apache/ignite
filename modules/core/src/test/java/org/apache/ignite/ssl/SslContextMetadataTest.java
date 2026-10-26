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
import javax.crypto.KeyGenerator;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.TestCertificateAuthority.certificateChain;
import static org.junit.Assert.assertArrayEquals;

/** Tests that certificate metadata belongs to the key material used to create a context. */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Store and key password of the generated test material. */
    private static final char[] PASSWORD = TestCertificateAuthority.PWD;

    /** Authority that signs the test certificates. */
    private TestCertificateAuthority ca;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("metadata-ca");
    }

    /** A fresh context loads the replacement store once and leaves the cached context's certificate metadata unchanged. */
    @Test
    public void testFreshContextUsesReplacementStoreAndPreservesCachedContext() throws Exception {
        KeyStore originalKeys = ca.issue();
        X509Certificate[] originalChain = certificateChain(originalKeys);
        StoreFactory factory = new StoreFactory(originalKeys);

        SSLContext cached = factory.create();

        assertEquals(1, factory.loads);
        assertArrayEquals(originalChain, reportedChain(cached));

        KeyStore replacementKeys = ca.issue();
        X509Certificate[] replacementChain = certificateChain(replacementKeys);

        factory.store = replacementKeys;

        SSLContext fresh = factory.createNew();

        assertNotSame(cached, fresh);
        assertEquals(2, factory.loads);
        assertArrayEquals(replacementChain, reportedChain(fresh));

        assertSame(cached, factory.create());
        assertArrayEquals(originalChain, reportedChain(cached));
        assertEquals("Returning the cached context must not load the store again", 2, factory.loads);
    }

    /** Both factory methods retain certificate metadata when they also apply TLS parameters. */
    @Test
    public void testMetadataWithTlsParameters() throws Exception {
        KeyStore store = ca.issue();
        StoreFactory factory = new StoreFactory(store);
        String[] protocols = {"TLSv1.2"};
        String[] ciphers = {"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"};

        factory.setProtocols(protocols);
        factory.setCipherSuites(ciphers);

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertArrayEquals(certificateChain(store), reportedChain(ctx));
            assertFalse("The loaded store contains a private key", reportsMissingPrivateKey(ctx));

            SSLEngine engine = ctx.createSSLEngine();

            assertArrayEquals(protocols, engine.getEnabledProtocols());
            assertArrayEquals(ciphers, engine.getEnabledCipherSuites());
        }
    }

    /** Multiple private keys leave the chain unknown but are not reported as missing. */
    @Test
    public void testMultiplePrivateKeysHaveUnknownChain() throws Exception {
        String secondKey = "second-key";
        KeyStore store = ca.issue();
        KeyStore other = ca.issue(secondKey);

        store.setKeyEntry(secondKey, other.getKey(secondKey, PASSWORD), PASSWORD, other.getCertificateChain(secondKey));

        StoreFactory factory = new StoreFactory(store);

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertNull(reportedChain(ctx));
            assertFalse("Two private keys are present even though the served chain is unknown", reportsMissingPrivateKey(ctx));
        }
    }

    /** Trusted certificates and secret keys do not count as private keys. */
    @Test
    public void testOnlyPrivateKeyEntriesCount() throws Exception {
        String keyAlias = "private-key";
        KeyStore keys = ca.issue(keyAlias);

        // JCEKS can hold both private keys and secret keys.
        KeyStore store = KeyStore.getInstance("JCEKS");

        store.load(null, PASSWORD);
        store.setCertificateEntry("trusted", keys.getCertificate(keyAlias));
        store.setEntry("secret", new KeyStore.SecretKeyEntry(KeyGenerator.getInstance("AES").generateKey()),
            new KeyStore.PasswordProtection(PASSWORD));

        StoreFactory factory = new StoreFactory(store);

        SSLContext withoutKey = factory.create();

        assertNull(reportedChain(withoutKey));
        assertTrue("Neither the trusted certificate nor the secret key is a private key", reportsMissingPrivateKey(withoutKey));

        store.setKeyEntry(keyAlias, keys.getKey(keyAlias, PASSWORD), PASSWORD, keys.getCertificateChain(keyAlias));

        SSLContext withKey = factory.createNew();

        assertArrayEquals(certificateChain(keys), reportedChain(withKey));
        assertFalse("The store now contains a private-key entry", reportsMissingPrivateKey(withKey));
    }

    /** Trust-only contexts can still be built for clients, and another parameters wrapper preserves the missing-key marker. */
    @Test
    public void testTrustOnlyContextCreation() throws Exception {
        StoreFactory factory = new StoreFactory(ca.trustStore());
        String[] protocols = {"TLSv1.2"};
        SSLParameters params = new SSLParameters();

        params.setProtocols(protocols);

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertNull(reportedChain(ctx));
            assertTrue(((SSLContextWrapper)ctx).isPrivateKeyMissing());

            SSLContextWrapper outer = new SSLContextWrapper(ctx, params);

            assertTrue(outer.isPrivateKeyMissing());
            assertArrayEquals(protocols, outer.createSSLEngine().getEnabledProtocols());
        }
    }

    /** Legacy hooks run for each new context, but not when create() returns its cached context. */
    @Test
    public void testLegacyHooksRunOncePerBuiltContext() {
        LegacyFactory factory = new LegacyFactory();

        SSLContext first = factory.create();

        assertNull(reportedChain(first));
        assertFalse("Legacy hooks do not report whether a private key is available", reportsMissingPrivateKey(first));
        assertEquals(1, factory.checks);
        assertEquals(1, factory.keys);
        assertEquals(1, factory.trust);

        assertSame(first, factory.create());

        assertEquals(1, factory.checks);
        assertEquals(1, factory.keys);
        assertEquals(1, factory.trust);

        SSLContext next = factory.createNew();

        assertNotSame(first, next);
        assertNull(reportedChain(next));
        assertFalse("Legacy hooks do not report whether a private key is available", reportsMissingPrivateKey(next));
        assertEquals(2, factory.checks);
        assertEquals(2, factory.keys);
        assertEquals(2, factory.trust);
    }

    /** A subclass can still obtain key managers from the file factory's legacy hook. */
    @Test
    public void testLegacyKeyManagerAdapter() throws Exception {
        StoreFactory factory = new StoreFactory(ca.issue());
        KeyManager[] mgrs = factory.legacyKeyManagers();

        assertNotNull(mgrs);
        assertTrue(mgrs.length > 0);
        assertEquals(1, factory.loads);
    }

    /** A factory that implements neither key-material hook reports the missing implementation. */
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

        String msg = "Subclass must override createKeyMaterial() or createKeyManagers()";

        Throwable err = GridTestUtils.assertThrows(log, factory::create, IgniteException.class, msg);

        assertTrue(err.getCause() instanceof SSLException);

        err = GridTestUtils.assertThrows(log, factory::createNew, IgniteException.class, msg);

        assertTrue(err.getCause() instanceof SSLException);
    }

    /** A subclass can supply material directly, and callers cannot replace its recorded managers or chain through arrays. */
    @Test
    public void testKeyMaterialDefensiveCopies() throws Exception {
        KeyStore keys = ca.issue();
        KeyManager[] mgrs = TestCertificateAuthority.keyManagers(keys);
        KeyManager mgr = mgrs[0];
        X509Certificate[] expected = certificateChain(keys);
        X509Certificate[] input = expected.clone();
        AbstractSslContextFactory.KeyMaterial material = new AbstractSslContextFactory.KeyMaterial(mgrs, input);

        mgrs[0] = null;
        input[0] = null;
        material.keyManagers()[0] = null;

        assertSame(mgr, material.keyManagers()[0]);

        AbstractSslContextFactory factory = new AbstractSslContextFactory() {
            /** {@inheritDoc} */
            @Override protected void checkParameters() {
                // No parameters are required by this factory.
            }

            /** {@inheritDoc} */
            @Override protected KeyMaterial createKeyMaterial() {
                return material;
            }

            /** {@inheritDoc} */
            @Override protected TrustManager[] createTrustManagers() {
                return null;
            }
        };

        assertArrayEquals(expected, reportedChain(factory.create()));
        assertArrayEquals(expected, reportedChain(factory.createNew()));
    }

    /** Neither callers nor another parameters wrapper can replace a context's recorded chain. */
    @Test
    public void testWrapperPreservesAndCopiesChain() throws Exception {
        X509Certificate[] expected = certificateChain(ca.issue());
        X509Certificate[] input = expected.clone();
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        SSLContextWrapper wrapper = new SSLContextWrapper(delegate, null, input);

        input[0] = null;
        wrapper.getCertificateChain()[0] = null;

        assertArrayEquals(expected, wrapper.getCertificateChain());

        String[] protocols = {"TLSv1.2"};
        SSLParameters params = new SSLParameters();

        params.setProtocols(protocols);

        SSLContextWrapper outer = new SSLContextWrapper(wrapper, params);

        assertArrayEquals(expected, outer.getCertificateChain());
        assertArrayEquals(protocols, outer.createSSLEngine().getEnabledProtocols());
    }

    /** Explicit metadata takes precedence over the wrapped context's metadata. */
    @Test
    public void testExplicitWrapperMetadataOverridesDelegateMetadata() throws Exception {
        X509Certificate[] expectedChain = certificateChain(ca.issue());
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        SSLContextWrapper keyless = new SSLContextWrapper(delegate, null, null, true);
        SSLContextWrapper withChain = new SSLContextWrapper(keyless, null, expectedChain);

        assertArrayEquals(expectedChain, withChain.getCertificateChain());
        assertFalse(withChain.isPrivateKeyMissing());

        SSLContextWrapper unknownChain = new SSLContextWrapper(withChain, null, null);

        assertNull(unknownChain.getCertificateChain());
        assertFalse(unknownChain.isPrivateKeyMissing());

        SSLContextWrapper explicitlyKeyless = new SSLContextWrapper(withChain, null, null, true);

        assertNull(explicitlyKeyless.getCertificateChain());
        assertTrue(explicitlyKeyless.isPrivateKeyMissing());
    }

    /** Adding metadata without TLS parameters preserves peer addresses, TLS defaults and unconnected socket creation. */
    @Test
    public void testMetadataWrapperPreservesContextBehavior() throws Exception {
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        SSLContextWrapper wrapper = new SSLContextWrapper(delegate, null, certificateChain(ca.issue()));
        String host = "peer.example";
        int port = 10800;
        SSLEngine engine = wrapper.createSSLEngine(host, port);
        SSLEngine delegateEngine = delegate.createSSLEngine();

        assertEquals(host, engine.getPeerHost());
        assertEquals(port, engine.getPeerPort());
        assertArrayEquals(delegateEngine.getEnabledProtocols(), engine.getEnabledProtocols());
        assertArrayEquals(delegateEngine.getEnabledCipherSuites(), engine.getEnabledCipherSuites());

        try (SSLSocket expected = (SSLSocket)delegate.getSocketFactory().createSocket();
            SSLSocket actual = (SSLSocket)wrapper.getSocketFactory().createSocket()) {
            assertFalse(actual.isConnected());
            assertFalse(actual.isBound());
            assertArrayEquals(expected.getEnabledProtocols(), actual.getEnabledProtocols());
            assertArrayEquals(expected.getEnabledCipherSuites(), actual.getEnabledCipherSuites());
        }

        try (SSLServerSocket expected = (SSLServerSocket)delegate.getServerSocketFactory().createServerSocket();
            SSLServerSocket actual = (SSLServerSocket)wrapper.getServerSocketFactory().createServerSocket()) {
            assertFalse(actual.isBound());
            assertArrayEquals(expected.getEnabledProtocols(), actual.getEnabledProtocols());
            assertArrayEquals(expected.getEnabledCipherSuites(), actual.getEnabledCipherSuites());
        }
    }

    /** @return Chain attached to the context, if known. */
    private static X509Certificate[] reportedChain(SSLContext ctx) {
        return ctx instanceof SSLContextWrapper ? ((SSLContextWrapper)ctx).getCertificateChain() : null;
    }

    /** @return Whether the context reports that no private key was loaded. Unknown metadata does not set this marker. */
    private static boolean reportsMissingPrivateKey(SSLContext ctx) {
        return ctx instanceof SSLContextWrapper && ((SSLContextWrapper)ctx).isPrivateKeyMissing();
    }

    /** A file factory whose existing store-loading hook supplies the test material. */
    private static class StoreFactory extends SslContextFactory {
        /** */
        private KeyStore store;

        /** */
        private int loads;

        /** */
        StoreFactory(KeyStore store) {
            this.store = store;

            setKeyStoreType(store.getType());
            setKeyStoreFilePath("test-store");
            setKeyStorePassword(PASSWORD);
            setTrustManagers(getDisabledTrustManager());
        }

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) {
            loads++;

            return store;
        }

        /** @return Key managers supplied by the inherited legacy hook. */
        @SuppressWarnings("deprecation")
        private KeyManager[] legacyKeyManagers() throws SSLException {
            return super.createKeyManagers();
        }
    }

    /** A subclass that implements the manager hooks without overriding createKeyMaterial(). */
    private static class LegacyFactory extends AbstractSslContextFactory {
        /** */
        private int checks;

        /** */
        private int keys;

        /** */
        private int trust;

        /** {@inheritDoc} */
        @Override protected void checkParameters() {
            checks++;
        }

        /** {@inheritDoc} */
        @Override protected KeyManager[] createKeyManagers() {
            keys++;

            return null;
        }

        /** {@inheritDoc} */
        @Override protected TrustManager[] createTrustManagers() {
            trust++;

            return null;
        }
    }
}
