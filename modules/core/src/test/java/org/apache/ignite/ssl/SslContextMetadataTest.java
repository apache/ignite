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

import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/** Tests that certificate metadata belongs to the key material used to create a context. */
public class SslContextMetadataTest extends GridCommonAbstractTest {
    /** Store and key password of the test fixtures. */
    private static final char[] PASSWORD = "123456".toCharArray();

    /** {@code createNew()} loads the store once, preserves the cached context and attaches the new chain to the new context. */
    @Test
    public void testMetadataUsesOneLoadAndStaysWithContext() throws Exception {
        KeyStore firstStore = loadStore("node01");
        StoreFactory factory = new StoreFactory(firstStore);

        SSLContext first = factory.create();

        assertArrayEquals(chain(firstStore, "node01"), chain(first));
        assertFalse(privateKeyMissing(first));
        assertEquals(1, factory.loads);

        factory.store = loadStore("node02");

        SSLContext next = factory.createNew();

        assertNotSame(first, next);
        assertSame(first, factory.create());
        assertEquals(2, factory.loads);
        assertArrayEquals(chain(firstStore, "node01"), chain(first));
        assertArrayEquals(chain(factory.store, "node02"), chain(next));
        assertFalse(privateKeyMissing(next));
    }

    /** Both factory methods retain certificate metadata when they also apply TLS parameters. */
    @Test
    public void testMetadataWithTlsParameters() throws Exception {
        KeyStore store = loadStore("node01");
        StoreFactory factory = new StoreFactory(store);
        String[] protocols = {"TLSv1.2"};
        String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};

        factory.setProtocols(protocols);
        factory.setCipherSuites(ciphers);

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertArrayEquals(chain(store, "node01"), chain(ctx));

            SSLEngine engine = ctx.createSSLEngine();

            assertArrayEquals(protocols, engine.getEnabledProtocols());
            assertArrayEquals(ciphers, engine.getEnabledCipherSuites());
        }
    }

    /** Multiple private keys leave the chain unknown but are not reported as missing. */
    @Test
    public void testMultiplePrivateKeysHaveUnknownChain() throws Exception {
        StoreFactory factory = new StoreFactory(loadStore("node0102"));

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertNull(chain(ctx));
            assertFalse(privateKeyMissing(ctx));
        }
    }

    /** Trusted certificates and secret keys do not count as private keys. */
    @Test
    public void testOnlyPrivateKeyEntriesCount() throws Exception {
        KeyStore keys = loadStore("node01");
        KeyStore store = KeyStore.getInstance("JCEKS");

        store.load(null, PASSWORD);
        store.setCertificateEntry("trusted", keys.getCertificate("node01"));
        store.setEntry("secret", new KeyStore.SecretKeyEntry(new SecretKeySpec(new byte[16], "AES")),
            new KeyStore.PasswordProtection(PASSWORD));

        StoreFactory factory = new StoreFactory(store);

        SSLContext withoutKey = factory.create();

        assertNull(chain(withoutKey));
        assertTrue(privateKeyMissing(withoutKey));

        store.setKeyEntry("key", keys.getKey("node01", PASSWORD), PASSWORD, keys.getCertificateChain("node01"));

        SSLContext withKey = factory.createNew();

        assertArrayEquals(chain(keys, "node01"), chain(withKey));
        assertFalse(privateKeyMissing(withKey));
    }

    /** Trust-only contexts can still be built for clients, and another parameters wrapper preserves the missing-key marker. */
    @Test
    public void testTrustOnlyContextCreation() throws Exception {
        StoreFactory factory = new StoreFactory(loadStore("trustboth"));

        for (SSLContext ctx : new SSLContext[] {factory.create(), factory.createNew()}) {
            assertNull(chain(ctx));
            assertTrue(((SSLContextWrapper)ctx).isPrivateKeyMissing());

            SSLParameters params = new SSLParameters();

            params.setProtocols(new String[] {"TLSv1.2"});

            SSLContextWrapper outer = new SSLContextWrapper(ctx, params);

            assertTrue(outer.isPrivateKeyMissing());
            assertArrayEquals(new String[] {"TLSv1.2"}, outer.createSSLEngine().getEnabledProtocols());
        }
    }

    /** A subclass that does not override createKeyMaterial() still supplies validation and managers on every build. */
    @Test
    public void testLegacyOverridesAreUsedWithoutMetadata() {
        LegacyFactory factory = new LegacyFactory();

        SSLContext first = factory.create();

        assertSame(first, factory.create());
        assertNotSame(first, factory.createNew());
        assertNull(chain(first));
        assertEquals(2, factory.checks);
        assertEquals(2, factory.keys);
        assertEquals(2, factory.trust);
    }

    /** A subclass can still obtain key managers from the file factory's legacy hook. */
    @Test
    public void testLegacyKeyManagerAdapter() throws Exception {
        StoreFactory factory = new StoreFactory(loadStore("node01"));
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
        KeyManager mgr = new KeyManager() { };
        KeyManager[] mgrs = {mgr};
        X509Certificate[] expected = chain(loadStore("node01"), "node01");
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

        assertArrayEquals(expected, chain(factory.create()));
        assertArrayEquals(expected, chain(factory.createNew()));
    }

    /** Neither callers nor another parameters wrapper can replace a context's recorded chain. */
    @Test
    public void testWrapperPreservesAndCopiesChain() throws Exception {
        X509Certificate[] expected = chain(loadStore("node01"), "node01");
        X509Certificate[] input = expected.clone();
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        SSLContextWrapper wrapper = new SSLContextWrapper(delegate, null, input);

        input[0] = null;
        wrapper.getCertificateChain()[0] = null;

        assertArrayEquals(expected, wrapper.getCertificateChain());

        SSLParameters params = new SSLParameters();

        params.setProtocols(new String[] {"TLSv1.2"});

        SSLContextWrapper outer = new SSLContextWrapper(wrapper, params);

        assertArrayEquals(expected, outer.getCertificateChain());
        assertArrayEquals(new String[] {"TLSv1.2"}, outer.createSSLEngine().getEnabledProtocols());
    }

    /** Adding metadata without TLS parameters preserves the peer address, default settings and socket factory classes. */
    @Test
    public void testMetadataWrapperPreservesContextBehavior() throws Exception {
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        SSLContextWrapper wrapper = new SSLContextWrapper(delegate, null, chain(loadStore("node01"), "node01"));
        SSLEngine engine = wrapper.createSSLEngine("peer.example", 10800);

        assertEquals("peer.example", engine.getPeerHost());
        assertEquals(10800, engine.getPeerPort());
        assertArrayEquals(delegate.createSSLEngine().getEnabledProtocols(), engine.getEnabledProtocols());
        assertArrayEquals(delegate.createSSLEngine().getEnabledCipherSuites(), engine.getEnabledCipherSuites());
        assertEquals(delegate.getSocketFactory().getClass(), wrapper.getSocketFactory().getClass());
        assertEquals(delegate.getServerSocketFactory().getClass(), wrapper.getServerSocketFactory().getClass());

        try (ServerSocket sock = wrapper.getServerSocketFactory().createServerSocket()) {
            assertFalse(sock.isBound());
        }
    }

    /** @return Loaded test key store. */
    private static KeyStore loadStore(String name) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");

        try (InputStream in = Files.newInputStream(Path.of(GridTestUtils.keyStorePath(name)))) {
            store.load(in, PASSWORD);
        }

        return store;
    }

    /** @return Chain stored for the alias. */
    private static X509Certificate[] chain(KeyStore store, String alias) throws Exception {
        Certificate[] chain = store.getCertificateChain(alias);

        return Arrays.copyOf(chain, chain.length, X509Certificate[].class);
    }

    /** @return Chain attached to the context, if known. */
    private static X509Certificate[] chain(SSLContext ctx) {
        return ctx instanceof SSLContextWrapper ? ((SSLContextWrapper)ctx).getCertificateChain() : null;
    }

    /** @return Whether the context reports that its key material has no private key. */
    private static boolean privateKeyMissing(SSLContext ctx) {
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
