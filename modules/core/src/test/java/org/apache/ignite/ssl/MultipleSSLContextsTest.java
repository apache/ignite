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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 * Test SSL configuration, where certificates for nodes, connectors and client connectors are signed using different
 * trust stores. SSL for all three transports are enabled at the same time.
 */
public class MultipleSSLContextsTest extends GridCommonAbstractTest {
    /** */
    private boolean clientMode = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        if (clientMode) {
            igniteCfg.setClientMode(true);

            igniteCfg.setSslContextFactory(clientSSLFactory());
        }
        else
            igniteCfg.setSslContextFactory(serverSSLFactory());

        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(clientConnectorSSLFactory());
        igniteCfg.setClientConnectorConfiguration(clientConnectorCfg);

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration()
            .setSslEnabled(true)
            .setSslFactory(connectorSSLFactory());
        igniteCfg.setConnectorConfiguration(connectorConfiguration);

        return igniteCfg;
    }

    /**
     * @return SSL context factory to use on server nodes for communication between nodes in a cluster.
     */
    private Factory<SSLContext> serverSSLFactory() {
        return GridTestUtils.sslTrustedFactory("server", "trustone");
    }

    /**
     * @return SSL context factory to use on client nodes for communication between nodes in a cluster.
     */
    private Factory<SSLContext> clientSSLFactory() {
        return GridTestUtils.sslTrustedFactory("client", "trustone");
    }

    /**
     * @return SSL context factory to use in client connectors.
     */
    private Factory<SSLContext> clientConnectorSSLFactory() {
        return GridTestUtils.sslTrustedFactory("thinServer", "trusttwo");
    }

    /**
     * @return SSL context factory to use in thin clients.
     */
    private Factory<SSLContext> thinClientSSLFactory() {
        return GridTestUtils.sslTrustedFactory("thinClient", "trusttwo");
    }

    /**
     * @param addr Address of a node to connect to.
     * @return {@link ClientConfiguration} that can be used to start a thin client.
     */
    private ClientConfiguration clientConfiguration(String addr) {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(addr);
        clientCfg.setSslContextFactory(thinClientSSLFactory());
        clientCfg.setSslMode(SslMode.REQUIRED);

        return clientCfg;
    }

    /**
     * @return SSL context factory to use in client connectors.
     */
    private Factory<SSLContext> connectorSSLFactory() {
        return GridTestUtils.sslTrustedFactory("connectorServer", "trustthree");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        clientMode = false;
        startGrids(3);
    }

    /**
     * Checks that thick clients with SSL enabled can join the cluster and perform some work on it.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThickClients() throws Exception {
        int clientsNum = 3;
        int keysNum = 1000;
        String cacheName = "thickClientCache";

        List<Ignite> clients = new ArrayList<>(clientsNum);

        clientMode = true;

        try {
            for (int i = 0; i < clientsNum; i++)
                clients.add(startGrid("client" + i));

            Map<Integer, Integer> expMap = new HashMap<>();

            for (int i = 0; i < keysNum; i++) {
                int clientId = keysNum % clientsNum;

                IgniteCache<Integer, Integer> cache = clients.get(clientId).getOrCreateCache(cacheName);

                cache.put(i, i);
                expMap.put(i, i);
            }

            IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);

            assertCacheContent(expMap, cache);
        }
        finally {
            for (Ignite client : clients)
                client.close();

            IgniteCache cache = grid(0).cache(cacheName);

            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * Checks that thin clients with SSL enabled can join the cluster and perform some work on it.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThinClients() throws Exception {
        int clientsNum = 3;
        int keysNum = 1000;
        String cacheName = "thinClientCache";

        List<IgniteClient> clients = new ArrayList<>(clientsNum);

        try {
            for (int i = 0; i < clientsNum; i++) {
                IgniteClient client = Ignition.startClient(clientConfiguration("127.0.0.1:1080" + i));

                clients.add(client);
            }

            Map<Integer, Integer> expMap = new HashMap<>();

            for (int i = 0; i < keysNum; i++) {
                int clientId = keysNum % clientsNum;

                ClientCache<Integer, Integer> cache = clients.get(clientId).getOrCreateCache(cacheName);

                cache.put(i, i);
                expMap.put(i, i);
            }

            IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);

            assertCacheContent(expMap, cache);
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
     * Checks that control.sh script can connect to the cluster, that has SSL enabled.
     */
    @Test
    public void testConnector() {
        CommandHandler hnd = new CommandHandler();

        int exitCode = hnd.execute(Arrays.asList(
            "--state",
            "--keystore", GridTestUtils.keyStorePath("connectorClient"),
            "--keystore-password", GridTestUtils.keyStorePassword(),
            "--truststore", GridTestUtils.keyStorePath("trustthree"),
            "--truststore-password", GridTestUtils.keyStorePassword()));

        assertEquals(EXIT_CODE_OK, exitCode);
    }

    /**
     * Checks that the {@code cache} has contents that math the provided map.
     *
     * @param exp A map with expected contents.
     * @param cache A cache to check.
     */
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
