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

import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test SSL interaction btw client and cluster
 */
public class SingleSSLThinClientTest extends GridCommonAbstractTest {

    /**
     * Configuration witn valid SSL params
     *
     * @param addr Address of a node to connect to.
     * @return {@link ClientConfiguration} that can be used to start a thin client.
     */
    private ClientConfiguration clientConfigurationSSLParam(String addr) {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(addr);
        clientCfg
                .setSslMode(SslMode.REQUIRED)
                .setSslTrustAll(false)
                .setSslProtocol(SslProtocol.TLS)
                .setSslKeyAlgorithm("SunX509")

        return clientCfg;
    }

    /**
     * Configuration with valid key params
     *
     * @param addr Address of a node to connect to.
     * @return {@link ClientConfiguration} that can be used to start a thin client.
     */
    private ClientConfiguration clientConfigurationKeysParam(String addr) {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(addr);
        clientCfg
                .setSslMode(SslMode.REQUIRED)
                .setSslClientCertificateKeyStorePath("client.jks")
                .setSslClientCertificateKeyStoreType("JKS")
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath("trust.jks")
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStoreType("JKS");

        return clientCfg;
    }

    /**
     * Checks client init with all valid SSL and key params
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConfigureSSLKeys() throws ClientException, Exception {

        ClientConfiguration clientCfg = clientConfigurationSSLParam("127.0.0.1:10800").
                .setSslClientCertificateKeyStorePath("client.jks")
                .setSslClientCertificateKeyStoreType("JKS")
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath("trust.jks")
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStoreType("JKS");

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
        }
        client.close();
    }

    /**
     * Checks client init with all valid SSL and non-valid key params
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConfigureSSLKeysSomeNotValid() throws ClientException, Exception {

        ClientConfiguration clientCfg = clientConfigurationSSLParam("127.0.0.1:10800").
                .setSslClientCertificateKeyStorePath("client.jks")
                .setSslClientCertificateKeyStoreType("JKS_")
                .setSslClientCertificateKeyStorePassword("123456_")
                .setSslTrustCertificateKeyStorePath("trust.jks")
                .setSslTrustCertificateKeyStoreType("JKS_");
                .setSslTrustCertificateKeyStorePassword("123456")

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
        }
        catch (ClientException | Exception e){
            return;
        }
        finally {
            client.close();
            throw new Exception ("Client init must be failed!");
        }
    }

    /**
     * Checks that thin clients with all valid SSL and key params can join the cluster and perform some work on it.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientSSLDoSomething() throws Exception {

        int keysNum = 1000;
        String cacheName = "thinClientCache";

        ClientConfiguration clientCfg = clientConfigurationSSLParam("127.0.0.1:10800").
                .setSslClientCertificateKeyStorePath("client.jks")
                .setSslClientCertificateKeyStoreType("JKS")
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(("trust.jks")
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStoreType("JKS");

        try {

            IgniteClient client = Ignition.startClient(clientCfg);

            Map<Integer, Integer> expMap = new HashMap<>();

            for (int i = 0; i < keysNum; i++) {

                ClientCache<Integer, Integer> cache = client.getOrCreateCache(cacheName);

                cache.put(i, i);
                expMap.put(i, i);
            }

            IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);

            assertCacheContent(expMap, cache);
        }
        catch (ClientException ex) {
            ex.printStackTrace();
        }
        finally {
            client.close();
            IgniteCache cache = grid(0).cache(cacheName);
            if (cache != null)
                cache.destroy();
        }
    }

}