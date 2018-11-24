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

package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownServiceException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for RestExecutor.
 */
public class RestExecutorSelfTest {
    /** Name of the cache created by default in the cluster. */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** Path to certificates and configs. */
    private static final String PATH_TO_RESOURCES = "modules/web-console/web-agent/src/test/resources/";

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

    /** */
    private static final String HTTP_URI = "http://localhost:8080";

    /** */
    private static final String HTTPS_URI = "https://localhost:8080";

    /** */
    private static final String JETTY_WITH_SSL = "jetty-with-ssl.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS = "jetty-with-ciphers.xml";

    /** This cipher is disabled by default in JDK 8. */
    private static final String MARKER_CIPHER = "TLS_DH_anon_WITH_AES_256_GCM_SHA384";

    /** */
    @Rule
    public final ExpectedException ruleForExpectedException = ExpectedException.none();

    /**
     * @return Base configuration for cluster node.
     */
    private IgniteConfiguration baseNodeConfiguration() {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.registerAddresses(Collections.singletonList(new InetSocketAddress("127.0.0.1", 47500)));

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, String> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(dfltCacheCfg);

        cfg.setIgniteInstanceName(UUID.randomUUID().toString());

        return cfg;
    }

    /**
     * @param jettyCfg Name of file with Jetty XML config.
     * @return Node configuration with enabled SSL for REST.
     */
    private IgniteConfiguration sslNodeConfiguration(String jettyCfg) {
        IgniteConfiguration cfg = baseNodeConfiguration();

        ConnectorConfiguration conCfg = new ConnectorConfiguration();
        conCfg.setJettyPath(resolvePath(jettyCfg));

        cfg.setConnectorConfiguration(conCfg);

        return cfg;
    }

    /**
     * Convert response to JSON.
     *
     * @param res REST result.
     * @return JSON object.
     * @throws IOException If failed to parse.
     */
    private JsonNode toJson(RestResult res) throws IOException {
        Assert.assertNotNull(res);

        String data = res.getData();

        Assert.assertNotNull(data);
        Assert.assertFalse(data.isEmpty());

        return MAPPER.readTree(data);
    }

    /**
     * @param file File name.
     * @return Path to file.
     */
    private String resolvePath(String file) {
        return PATH_TO_RESOURCES + file;
    }

    /**
     * Try to execute REST command and check response.
     *
     * @param nodeCfg Node configuration.
     * @param uri Node URI.
     * @param keyStore Key store.
     * @param keyStorePwd Key store password.
     * @param trustStore Trust store.
     * @param trustStorePwd Trust store password.
     * @param cipherSuites Cipher suites.
     * @throws Exception If failed.
     */
    private void checkRest(
        IgniteConfiguration nodeCfg,
        String uri,
        String keyStore,
        String keyStorePwd,
        String trustStore,
        String trustStorePwd,
        String cipherSuites
    ) throws Exception {
        try(
            Ignite ignite = Ignition.getOrStart(nodeCfg);
            RestExecutor exec = new RestExecutor(keyStore, keyStorePwd, trustStore, trustStorePwd, cipherSuites)
        ) {
            Map<String, Object> params = new HashMap<>();
            params.put("cmd", "top");
            params.put("attr", false);
            params.put("mtr", false);
            params.put("caches", false);

            RestResult res = exec.sendRequest(Collections.singletonList(uri), params, null);

            JsonNode json = toJson(res);

            Assert.assertTrue(json.isArray());

            for (JsonNode item : json) {
                Assert.assertTrue(item.get("attributes").isNull());
                Assert.assertTrue(item.get("metrics").isNull());
                Assert.assertTrue(item.get("caches").isNull());
            }
        }
    }

    /** */
    @Test
    public void nodeNoSslAgentNoSsl() throws Exception {
        checkRest(
            baseNodeConfiguration(),
            HTTP_URI,
            null, null,
            null, null,
            null
        );
    }

    /** */
    @Test
    public void nodeNoSslAgentWithSsl() throws Exception {
        // Check Web Agent with SSL.
        ruleForExpectedException.expect(SSLHandshakeException.class);
        checkRest(
            baseNodeConfiguration(),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            null
        );
    }

    /** */
    @Test
    public void nodeWithSslAgentNoSsl() throws Exception {
        ruleForExpectedException.expect(IOException.class);
        checkRest(
            sslNodeConfiguration(JETTY_WITH_SSL),
            HTTP_URI,
            null, null,
            null, null,
            null
        );
    }

    /** */
    @Test
    public void nodeWithSslAgentWithSsl() throws Exception {
        checkRest(
            sslNodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            null
        );
    }

    /** */
    @Test
    public void nodeNoCiphersAgentWithCiphers() throws Exception {
        ruleForExpectedException.expect(UnknownServiceException.class);
        checkRest(
            sslNodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            MARKER_CIPHER
        );
   }

    /** */
    @Test
    public void nodeWithCiphersAgentNoCiphers() throws Exception {
        ruleForExpectedException.expect(SSLHandshakeException.class);
        checkRest(
            sslNodeConfiguration(JETTY_WITH_CIPHERS),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            null
        );
   }

    /** */
    @Test
    public void nodeWithCiphersAgentWithCiphers() throws Exception {
        checkRest(
            sslNodeConfiguration(JETTY_WITH_CIPHERS),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            MARKER_CIPHER
        );
   }
}
