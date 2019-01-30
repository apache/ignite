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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
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
    private static final String JETTY_WITH_CIPHERS_0 = "jetty-with-ciphers-0.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS_1 = "jetty-with-ciphers-1.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS_2 = "jetty-with-ciphers-2.xml";

    /** This cipher is disabled by default in JDK 8. */
    private static final List<String> CIPHER_0 = Collections.singletonList("TLS_DH_anon_WITH_AES_256_GCM_SHA384");

    /** */
    private static final List<String> CIPHER_1 = Collections.singletonList("TLS_RSA_WITH_NULL_SHA256");

    /** */
    private static final List<String> CIPHER_2 = Collections.singletonList("TLS_ECDHE_ECDSA_WITH_NULL_SHA");

    /** */
    private static final List<String> COMMON_CIPHERS = Arrays.asList(
        "TLS_RSA_WITH_NULL_SHA256",
        "TLS_ECDHE_ECDSA_WITH_NULL_SHA"
    );

    /** */
    @Rule
    public final ExpectedException ruleForExpectedException = ExpectedException.none();

    /**
     * @param jettyCfg Optional path to file with Jetty XML config.
     * @return Prepare configuration for cluster node.
     */
    private IgniteConfiguration nodeConfiguration(String jettyCfg) {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.registerAddresses(Collections.singletonList(new InetSocketAddress("127.0.0.1", 47500)));

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, String> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(dfltCacheCfg);

        cfg.setIgniteInstanceName(UUID.randomUUID().toString());

        if (!F.isEmpty(jettyCfg)) {
            ConnectorConfiguration conCfg = new ConnectorConfiguration();
            conCfg.setJettyPath(resolvePath(jettyCfg));

            cfg.setConnectorConfiguration(conCfg);
        }

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
        return IgniteUtils.resolveIgnitePath(PATH_TO_RESOURCES + file).getAbsolutePath();
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
        List<String> cipherSuites
    ) throws Exception {
        try(
            Ignite ignite = Ignition.getOrStart(nodeCfg);
            RestExecutor exec = new RestExecutor(false, keyStore, keyStorePwd, trustStore, trustStorePwd, cipherSuites)
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
            nodeConfiguration(""),
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
        ruleForExpectedException.expect(SSLException.class);
        checkRest(
            nodeConfiguration(""),
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
            nodeConfiguration(JETTY_WITH_SSL),
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
            nodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            null
        );
    }

    /** */
    @Test
    public void nodeNoCiphersAgentWithCiphers() throws Exception {
        ruleForExpectedException.expect(SSLHandshakeException.class);
        checkRest(
            nodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            CIPHER_0
        );
   }

    /** */
    @Test
    public void nodeWithCiphersAgentNoCiphers() throws Exception {
        ruleForExpectedException.expect(SSLHandshakeException.class);
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_0),
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
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            CIPHER_1
        );
   }

    /** */
    @Test
    public void differentCiphers1() throws Exception {
        ruleForExpectedException.expect(SSLHandshakeException.class);
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            CIPHER_2
        );
   }

    /** */
    @Test
    public void differentCiphers2() throws Exception {
        ruleForExpectedException.expect(SSLException.class);
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_2),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            CIPHER_1
        );
   }

    /** */
    @Test
    public void commonCiphers() throws Exception {
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            resolvePath("ca.jks"), "123456",
            COMMON_CIPHERS
        );
   }
}
