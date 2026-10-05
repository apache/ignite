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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests the owner of an SSL context: it has to hand out one context until told to reload, and to pick the rotated
 * stores up when it is.
 */
public class SslContextProviderTest extends GridCommonAbstractTest {
    /** Key store the provider reads; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-ssl-provider-", ".jks");

        placeStore("node01");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Files.deleteIfExists(keyStore);
    }

    /** Until it is reloaded, the provider must hand out one and the same context. */
    @Test
    public void testContextStaysUntilReloaded() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext inUse = provider.context();

        // The context in use must not be rebuilt behind the caller's back.
        assertSame(inUse, provider.context());

        placeStore("node02");

        // A rotated store must not reach connections before the reload does.
        assertSame(inUse, provider.context());
    }

    /** A reload must read the stores again and put what they hold now in use. */
    @Test
    public void testReloadPutsRotatedStoreInUse() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext before = provider.context();

        placeStore("node02");

        assertTrue("A rotated store must be reported as reloaded", provider.reload());

        // Connections opened afterwards must use the rotated store.
        assertNotSame(before, provider.context());
    }

    /** A factory that keeps handing back one context has nothing to put in use, and the provider must say so. */
    @Test
    public void testReadyMadeContextReportedAsNothingToReload() throws Exception {
        SSLContext readyMade = fileFactory().create();

        SslContextProvider provider = new SslContextProvider(() -> readyMade);

        assertFalse("A context handed over ready-made cannot be reloaded", provider.reload());

        assertSame(readyMade, provider.context());
    }

    /** A check must tell that the stores can be used, and leave the context in use alone. */
    @Test
    public void testCheckLeavesContextInUse() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext before = provider.context();

        placeStore("node02");

        assertTrue("A rotated store must be reported as usable", provider.check());

        assertSame(before, provider.context());
    }

    /** A store that cannot be read must fail the reload and leave the context in use alone. */
    @Test
    public void testBrokenStoreKeepsContextInUse() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext before = provider.context();

        Files.write(keyStore, "not a key store".getBytes());

        GridTestUtils.assertThrowsWithCause(() -> provider.reload(), SSLException.class);

        assertSame(before, provider.context());
    }

    /**
     * Where nodes connect to each other, a certificate the provider's own trust store refuses must not be put in use,
     * and the failure must name the certificate.
     */
    @Test
    public void testUntrustedCertificateNotApplied() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory("trustone"));

        provider.addUser(SslContextReloadable.COMMUNICATION, true);

        SSLContext before = provider.context();

        // node02 is issued by "twoca", which the "trust-one" store does not contain.
        placeStore("node02");

        Throwable e = GridTestUtils.assertThrows(log, () -> provider.reload(), IgniteCheckedException.class,
            "A handshake between nodes on the new certificate was refused");

        assertContains(log, e.getMessage(), "subject=CN=node02");

        assertSame(before, provider.context());
    }

    /** The certificate in use must be told for any provider, not only for the ones that connect nodes. */
    @Test
    public void testServedCertificateOfClientFacingProvider() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        provider.addUser(SslContextReloadable.CLIENT_CONNECTOR, false);

        assertEquals("CN=node01", provider.servedCertificate().getSubjectX500Principal().getName());
    }

    /**
     * @return Factory reading the store this test rotates.
     */
    private Factory<SSLContext> fileFactory() {
        return fileFactory(null);
    }

    /**
     * @param trustStore Test trust store to trust peers by, {@code null} to trust any peer.
     * @return Factory reading the store this test rotates.
     */
    private Factory<SSLContext> fileFactory(@Nullable String trustStore) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        if (trustStore == null)
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        else {
            factory.setTrustStoreFilePath(GridTestUtils.keyStorePath(trustStore));
            factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        }

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void placeStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }
}
