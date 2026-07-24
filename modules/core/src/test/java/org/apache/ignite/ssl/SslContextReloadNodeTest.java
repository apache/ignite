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
import java.util.Arrays;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests hot reload of TLS certificates on a running node via the {@code --ssl reload} command task: the SSL-enabled
 * transports must reload after the key store on disk has been replaced, while the cluster keeps operating
 * (established sessions are not interrupted).
 */
public class SslContextReloadNodeTest extends GridCommonAbstractTest {
    /** Key store file shared by the nodes; replaced on disk to simulate certificate rotation. */
    private Path keyStore;

    /** Whether SSL should be configured for the node being started. */
    private boolean ssl = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (ssl) {
            cfg.setSslContextFactory(reloadableFactory());

            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(false)
                .setUseIgniteSslContextFactory(true));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("advui-node-reload-", ".jks");

        copyKeyStore("node01");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** Certificate reload on every SSL transport of a running two-node cluster must succeed and keep it operational. */
    @Test
    public void testReloadOnRunningCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        assertEquals((Integer)1, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));

        // Rotate the certificate on disk.
        copyKeyStore("node02");

        String res = reload(g0, g0.localNode().id(), g1.localNode().id());

        assertTrue(res, res.contains("communication"));
        assertTrue(res, res.contains("discovery"));
        assertTrue(res, res.contains("client connector"));

        // Established sessions are not interrupted: the cluster keeps working after the reload.
        cache.put(2, 2);

        assertEquals((Integer)2, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(2));
    }

    /** Reload must report nothing to reload on a node that does not use SSL. */
    @Test
    public void testReloadWithoutSsl() throws Exception {
        ssl = false;

        try {
            IgniteEx g = startGrid(0);

            String res = reload(g, g.localNode().id());

            assertTrue(res, res.contains("SSL is not configured"));
        }
        finally {
            ssl = true;
        }
    }

    /**
     * Executes the {@code --ssl reload} task on the given nodes.
     *
     * @param ignite Node to submit the task from.
     * @param nodeIds Nodes to reload certificates on.
     * @return Aggregated task result.
     */
    private String reload(IgniteEx ignite, UUID... nodeIds) throws Exception {
        VisorTaskResult<String> res = ignite.compute().execute(SslReloadTask.class,
            new VisorTaskArgument<>(Arrays.asList(nodeIds), new NoArg(), false));

        return res.result();
    }

    /**
     * @return SSL context factory that reads its certificate from {@link #keyStore} (so a reload picks up an
     *      updated file) and trusts any peer certificate.
     */
    private Factory<SSLContext> reloadableFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void copyKeyStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }
}
