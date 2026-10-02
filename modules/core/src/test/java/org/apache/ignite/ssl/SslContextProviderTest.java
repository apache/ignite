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
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

    /** What one attempt prepared must not be applied by another: the operator was shown different certificates. */
    @Test
    public void testCommitAppliesOnlyWhatTheSameAttemptPrepared() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext before = provider.context();

        placeStore("node02");

        assertTrue(provider.prepare(UUID.randomUUID()));

        assertEquals("A foreign attempt must not apply what this one prepared",
            SslContextReloadable.Commit.NOT_PREPARED, provider.commit(UUID.randomUUID()));

        // Nothing was applied, and nothing was thrown away either.
        assertSame(before, provider.context());
    }

    /** A dry run keeps nothing: what it built must not become applicable later. */
    @Test
    public void testDiscardLeavesNothingToApply() throws Exception {
        SslContextProvider provider = new SslContextProvider(fileFactory());

        SSLContext before = provider.context();

        placeStore("node02");

        UUID token = UUID.randomUUID();

        assertTrue(provider.prepare(token));

        provider.discard();

        assertEquals("Discarded work must not be applicable",
            SslContextReloadable.Commit.NOT_PREPARED, provider.commit(token));

        assertSame(before, provider.context());
    }

    /**
     * @return Factory reading the store this test rotates.
     */
    private Factory<SSLContext> fileFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void placeStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }
}
