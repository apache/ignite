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

package org.apache.ignite.internal.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;

/**
 * Tests that client is able to connect to a grid with only default cache enabled.
 */
public class ClientDefaultCacheSelfTest extends GridCommonAbstractTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** Cached local node id. */
    private UUID locNodeId;

    /** Http port. */
    private static final int HTTP_PORT = 8081;

    /** Url address to send HTTP request. */
    private static final String TEST_URL = "http://" + HOST + ":" + HTTP_PORT + "/ignite";

    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** Name of node local cache. */
    private static final String LOCAL_CACHE = "local";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(HTTP_PORT));

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();

        System.clearProperty(IGNITE_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        locNodeId = grid().localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setJettyPath(REST_JETTY_CFG);

        cfg.setConnectorConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cLocal = new CacheConfiguration();

        cLocal.setName(LOCAL_CACHE);

        cLocal.setCacheMode(CacheMode.LOCAL);

        cLocal.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(defaultCacheConfiguration(), cLocal);

        return cfg;
    }

    /**
     * Builds list of connection strings with few different ports.
     * Used to avoid possible failures in case of port range active.
     *
     * @param startPort Port to start list from.
     * @return List of client connection strings.
     */
    private Collection<String> getServerList(int startPort) {
        Collection<String> srvs = new ArrayList<>();

        for (int i = startPort; i < startPort + 10; i++)
            srvs.add(HOST + ":" + i);

        return srvs;
    }

    /*
     * Send HTTP request to Jetty server of node and process result.
     *
     * @param query Send query parameters.
     * @return Processed response string.
     */
    private String sendHttp(String query) {
        String res = "No result";

        try {
            URLConnection connection = new URL(TEST_URL + "?" + query).openConnection();

            connection.setRequestProperty("Accept-Charset", CHARSET);

            BufferedReader r = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            res = r.readLine();

            r.close();
        }
        catch (IOException e) {
            error("Failed to send HTTP request: " + TEST_URL + "?" + query, e);
        }

        // Cut node id from response.
        return res.substring(res.indexOf("\"response\""));
    }

    /**
     * Json format string in cache should not transform to Json object on get request.
     */
    public void testSkipString2JsonTransformation() {
        // Put to cache JSON format string value.
        assertEquals("Incorrect query response", "\"response\":true,\"sessionToken\":\"\",\"successStatus\":0}",
            sendHttp("cmd=put&cacheName=" + LOCAL_CACHE +
                "&key=a&val=%7B%22v%22%3A%22my%20Value%22%2C%22t%22%3A1422559650154%7D"));

        // Escape '\' symbols disappear from response string on transformation to JSON object.
        assertEquals(
            "Incorrect query response",
            "\"response\":\"{\\\"v\\\":\\\"my Value\\\",\\\"t\\\":1422559650154}\"," +
                "\"sessionToken\":\"\",\"successStatus\":0}",
            sendHttp("cmd=get&cacheName=" + LOCAL_CACHE + "&key=a"));
    }
}