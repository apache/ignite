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

package org.apache.ignite.internal.ssl;

import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.AbstractSslContextFactory.ReloadOutcome;
import org.apache.ignite.ssl.SSLContextWrapper;
import org.apache.ignite.ssl.SslBundle;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextRegistry.BINARY_REST;
import static org.apache.ignite.internal.ssl.SslContextRegistry.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.TestCertificateAuthority.bundle;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.context;
import static org.apache.ignite.testframework.TestCertificateAuthority.merged;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The provider exposes the material and reload results owned by its factory. Reload validates the complete candidate before
 * replacing the material in use. The registry shares a provider between transports using the same factory object.
 */
public class SslContextProviderTest extends GridCommonAbstractTest {
    /** */
    private static final long HOUR = 60 * 60_000L;

    /** Authority that signs the certificates the providers start on. */
    private TestCertificateAuthority ca;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("provider-ca");
    }

    /**
     * Reload installs the context and metadata of a new factory call; the factory is called once on creation and once per reload.
     * The new keys come from another authority: reload does not check them against the trust of the node.
     */
    @Test
    public void testReloadInstallsNewBundle() throws Exception {
        TestCertificateAuthority otherCa = new TestCertificateAuthority("other-ca");
        KeyStore a = otherCa.keyStoreWithPrivateKey("a");
        KeyStore b = otherCa.keyStoreWithPrivateKey("b");
        SslBundle next = bundle(merged(a, b), ca.trustStore());
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()), next).when(f).createSslBundle();

        SslContextProvider p = new SslContextProvider(f);

        p.reload();

        assertSame(next.context(), p.context());
        assertEquals(Map.of("a", chain(a), "b", chain(b)), p.certificateChains());
        verify(f, times(2)).createSslBundle();
        assertTrue(p.reloadOutcome().successTime() > 0);
    }

    /** Calling the factory directly updates the provider's material and outcome without replacing its shared context. */
    @Test
    public void testDirectFactoryInitUpdatesProvider() throws Exception {
        SslBundle next = bundle(ca.keyStoreWithPrivateKey("replacement"), ca.trustStore());
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()), next).when(f).createSslBundle();

        SslContextProvider p = new SslContextProvider(f);
        SSLContext shared = p.sharedContext();

        assertEquals(new ReloadOutcome(0, 0, null, 0), p.reloadOutcome());

        f.init();

        assertSame(next, p.bundle());
        assertSame(next.context(), p.context());
        assertEquals(next.certificateChains(), p.certificateChains());
        assertSame(shared, p.sharedContext());
        assertSame(f.reloadOutcome(), p.reloadOutcome());
        assertTrue(p.reloadOutcome().successTime() > 0);
    }

    /**
     * A failed reload keeps the context, metadata and success time in use, counts failures in a row and records the time and the
     * reason without the messages of wrappers; a successful reload clears the failures.
     */
    @Test
    public void testReloadFailureRetainsContextUntilRecovery() throws Exception {
        SslBundle reloaded = bundle(ca.keyStoreWithPrivateKey("reloaded"), ca.trustStore());
        KeyStore recoveredKeys = ca.keyStoreWithPrivateKey("recovered");
        IgniteException failure = new IgniteException("Failed to load", new SSLException(new IOException("bad file")));
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()), reloaded)
            .doThrow(failure, failure)
            .doReturn(bundle(recoveredKeys, ca.trustStore()))
            .when(f).createSslBundle();

        SslContextProvider p = new SslContextProvider(f);

        // A successful reload first, so that the success time the failures must keep is not 0.
        p.reload();

        long successTime = p.reloadOutcome().successTime();
        long failureStart = System.currentTimeMillis();

        GridTestUtils.assertThrows(log, p::reload, IgniteException.class, "Failed to load");
        GridTestUtils.assertThrows(log, p::reload, IgniteException.class, "Failed to load");

        ReloadOutcome failed = p.reloadOutcome();

        assertSame(reloaded.context(), p.context());
        assertEquals(reloaded.certificateChains(), p.certificateChains());
        assertTrue(failed.failureTime() >= failureStart);
        assertEquals(new ReloadOutcome(successTime, failed.failureTime(), "Failed to load: bad file", 2), failed);

        p.reload();

        ReloadOutcome recovered = p.reloadOutcome();

        assertEquals(Map.of("recovered", chain(recoveredKeys)), p.certificateChains());
        assertTrue(recovered.successTime() >= failed.failureTime());
        assertEquals(new ReloadOutcome(recovered.successTime(), 0, null, 0), recovered);
    }

    /** Key material with no certificate chains, such as a trust store put in place of the key store, is not applied. */
    @Test
    public void testMaterialWithoutCertificateChainsNotApplied() throws Exception {
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()),
            bundle(ca.trustStore(), ca.trustStore())).when(f).createSslBundle();

        assertReloadRejected(new SslContextProvider(f),
            "The new key material has no X.509 certificate chains, so the node would present no certificate");
    }

    /** A context that cannot create an engine, here with a protocol the JVM does not support, is not applied. */
    @Test
    public void testUnsupportedProtocolNotApplied() throws Exception {
        SslBundle supported = bundle(ca.keyStoreWithPrivateKey("next"), ca.trustStore());
        SSLParameters params = new SSLParameters();

        params.setProtocols(new String[] {"SSLvDoesNotExist"});

        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()),
            new SslBundle(new SSLContextWrapper(supported.context(), params), supported.certificateChains()))
            .when(f).createSslBundle();

        assertReloadRejected(new SslContextProvider(f), "SSLvDoesNotExist");
    }

    /** A factory that returns the context in use, as a caching custom factory does, gives the node no new certificates. */
    @Test
    public void testContextInUseNotReloaded() throws Exception {
        SSLContext ctx = context(ca.keyStoreWithPrivateKey(), ca.trustStore());

        assertReloadRejected(new SslContextProvider(() -> ctx), "The SSL context factory returns the context in use");
    }

    /** Every certificate of every chain must be valid now: a later key that is not valid yet and an expired authority are refused. */
    @Test
    public void testEveryCertificateMustBeValidNow() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore valid = ca.keyStoreWithPrivateKey("a-valid");
        KeyStore notYetValid = ca.keyStoreWithPrivateKey("z-not-yet-valid", now + HOUR, now + 2 * HOUR);
        KeyStore expiredCaLeaf = new TestCertificateAuthority("expired-ca", now - 2 * HOUR, now - HOUR).keyStoreWithPrivateKey("leaf");
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()),
            bundle(merged(valid, notYetValid), ca.trustStore()),
            bundle(expiredCaLeaf, ca.trustStore())).when(f).createSslBundle();

        SslContextProvider p = new SslContextProvider(f);

        assertReloadRejected(p, "The new certificate chain is not valid now [alias=z-not-yet-valid, subject=CN=z-not-yet-valid,");
        assertReloadRejected(p, "The new certificate chain is not valid now [alias=leaf, subject=CN=expired-ca,");
    }

    /**
     * The registry keeps one provider per factory object: transports of one factory share it, while an equal factory, here a record
     * over the same context, gets its own.
     */
    @Test
    public void testRegistryKeepsProviderPerFactoryObject() throws Exception {
        /** Factory that always returns the same context; records with the same context are equal. */
        record Fixed(SSLContext ctx) implements Factory<SSLContext> {
            /** {@inheritDoc} */
            @Override public SSLContext create() {
                return ctx;
            }
        }

        SSLContext ctx = context(ca.keyStoreWithPrivateKey(), ca.trustStore());
        Fixed shared = new Fixed(ctx);
        SslContextRegistry registry = new SslContextRegistry(new StandaloneGridKernalContext(log, null));

        SSLContext communication = registry.register(shared, COMMUNICATION);
        SSLContext discovery = registry.register(shared, DISCOVERY);
        SSLContext binaryRest = registry.register(new Fixed(ctx), BINARY_REST);

        assertSame(communication, discovery);
        assertNotSame(communication, binaryRest);
        assertEquals(2, registry.providers().size());
    }

    /** Registration warns when the key material has no certificate chains, here a trust store put in place of the key store. */
    @Test
    public void testRegistryWarnsAboutMissingCertificateChains() throws Exception {
        AbstractSslContextFactory f = mock(AbstractSslContextFactory.class, CALLS_REAL_METHODS);

        doReturn(bundle(ca.trustStore(), ca.trustStore())).when(f).createSslBundle();

        ListeningTestLogger registryLog = new ListeningTestLogger(log);
        LogListener warned = LogListener.matches("The key material has no X.509 certificate chains, " +
            "so the node cannot present a certificate; reload rejects such key material").times(1).build();

        registryLog.registerListener(warned);

        SslContextRegistry registry = new SslContextRegistry(new StandaloneGridKernalContext(registryLog, null));

        registry.register(f, CLIENT_CONNECTOR);

        assertTrue(warned.check());
    }

    /**
     * Checks that reload fails with the reason in its message and in the recorded outcome, keeps the context and metadata in use
     * and counts one more failure.
     *
     * @param p Provider.
     * @param reason Expected part of the failure message; also names the case in assertion messages.
     */
    private void assertReloadRejected(SslContextProvider p, String reason) {
        SSLContext inUse = p.context();
        Map<String, List<X509Certificate>> chains = p.certificateChains();
        int failures = p.reloadOutcome().failures();

        GridTestUtils.assertThrows(log, p::reload, Exception.class, reason);

        Assert.assertSame(reason, inUse, p.context());
        assertEquals(reason, chains, p.certificateChains());
        assertEquals(reason, failures + 1, p.reloadOutcome().failures());
        assertContains(log, p.reloadOutcome().failureReason(), reason);
    }
}
