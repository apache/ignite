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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ThinClientSSLTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setSslContextFactory(serverSSLFactory());

        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(true)
                .setUseIgniteSslContextFactory(false)
                .setSslContextFactory(clientConnectorSSLFactory());
        igniteCfg.setClientConnectorConfiguration(clientConnectorCfg);

        return igniteCfg;
    }

    /**
     * @return SSL context factory to use on server nodes for communication between nodes in a cluster.
     */
    private Factory<SSLContext> serverSSLFactory() {
        return GridTestUtils.sslTrustedFactory("server", "trustone");
    }

    /**
     * @return SSL context factory to use in client connectors.
     */
    private Factory<SSLContext> clientConnectorSSLFactory() {
        return GridTestUtils.sslTrustedFactory("thinServer", "trusttwo");
    }

    private ClientConfiguration clientConfigurationWithoutSslContextFactory(String addr) {
        return new ClientConfiguration().setAddresses(addr)
                .setSslClientCertificateKeyStorePath(GridTestUtils.keyStorePath("thinClient"))
                .setSslTrustCertificateKeyStorePath(GridTestUtils.keyStorePath("trustone"))
                .setSslClientCertificateKeyStorePassword(GridTestUtils.keyStorePassword())
                .setSslTrustCertificateKeyStorePassword(GridTestUtils.keyStorePassword())
                .setSslMode(SslMode.REQUIRED)
                .setSslTrustAll(true);
    }

    private ClientConfiguration clientConfigurationWithSslContextFactory(String addr) {
        return new ClientConfiguration().setAddresses(addr)
                .setSslContextFactory(SSLContextFactoryForTests.thinClientSSLFactoryWithWrongTrustCertificate())
                .setSslMode(SslMode.REQUIRED)
                .setSslTrustAll(true);
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /**
     * Checks that thin clients with SSL enabled and SetSSLTrust(true) without set SslContextFactory can join the cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetSSLTrustAllTrueWithoutSslContextFactory() throws Exception {
        String cacheName = "thinClientCache";
        List<IgniteClient> clients = new ArrayList<>(1);

        try {
            IgniteClient client = Ignition.startClient(clientConfigurationWithoutSslContextFactory("127.0.0.1:10800"));

            Map<Integer, Integer> expMap = new HashMap<>();

            ClientCache<Integer, Integer> cache = client.getOrCreateCache(cacheName);

            cache.put(1, 1);
            expMap.put(1, 1);

            IgniteCache<Integer, Integer> cache1 = grid(0).cache(cacheName);

            assertCacheContent(expMap, cache1);
        }
        catch (ClientException ex) {
            ex.printStackTrace();

            fail("Failed to start thin Java clients: " + ex.getMessage());
        }
        finally {
            for (IgniteClient client : clients)
                client.close();

            IgniteCache cache = grid(0).cache(cacheName);

            if (cache != null)
                cache.destroy();
        }
    }


    /**
     * Checks that thin clients with SSL enabled and SetSSLTrust(true) with set SslContextFactory can join the cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetSSLTrustAllTrueWithSslContextFactory() throws Exception {
        String cacheName = "thinClientCache";
        List<IgniteClient> clients = new ArrayList<>(1);

        try {
            IgniteClient client = Ignition.startClient(clientConfigurationWithSslContextFactory("127.0.0.1:10800"));

            Map<Integer, Integer> expMap = new HashMap<>();

            ClientCache<Integer, Integer> cache = client.getOrCreateCache(cacheName);

            cache.put(1, 1);
            expMap.put(1, 1);

            IgniteCache<Integer, Integer> cache1 = grid(0).cache(cacheName);

            assertCacheContent(expMap, cache1);
        }
        catch (ClientException ex) {
            ex.printStackTrace();

            fail("Failed to start thin Java clients: " + ex.getMessage());
        }
        finally {
            for (IgniteClient client : clients)
                client.close();

            IgniteCache cache = grid(0).cache(cacheName);

            if (cache != null)
                cache.destroy();
        }
    }

    private void assertCacheContent(Map<Integer, Integer> exp, IgniteCache<Integer, Integer> cache) {
        assertEquals("Cache has an unexpected size.", exp.size(), cache.size());
        for (Map.Entry<Integer, Integer> e : exp.entrySet()) {
            int key = e.getKey();
            Integer expVal = e.getValue();
            Integer actVal = cache.get(key);

            assertEquals("Cache contains an unexpected value for a key=" + key, expVal, actVal);
        }
    }
}
