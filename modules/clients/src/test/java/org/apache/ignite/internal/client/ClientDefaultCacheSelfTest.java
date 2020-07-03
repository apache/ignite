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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;

/**
 * Tests that client is able to connect to a grid with only default cache enabled.
 */
public class ClientDefaultCacheSelfTest extends GridCommonAbstractTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** Cached local node id. */
    private UUID locNodeId;

    /** Http port. */
    private static final int HTTP_PORT = 8081;

    /** Url address to send HTTP request. */
    private static final String TEST_URL = "http://" + HOST + ":" + HTTP_PORT + "/ignite?";

    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** Name of node local cache. */
    private static final String LOCAL_CACHE = "local";

    /** JSON to java mapper. */
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(HTTP_PORT));

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IGNITE_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        locNodeId = grid().localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setJettyPath(REST_JETTY_CFG);

        cfg.setConnectorConfiguration(clientCfg);

        CacheConfiguration cLoc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cLoc.setName(LOCAL_CACHE);

        cLoc.setCacheMode(CacheMode.LOCAL);

        cLoc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(defaultCacheConfiguration(), cLoc);

        return cfg;
    }

    /**
     * Send HTTP request to Jetty server of node and process result.
     *
     * @param params Command parameters.
     * @return Processed response string.
     * @throws IOException If failed.
     */
    private String content(Map<String, String> params) throws IOException {
        SB sb = new SB(TEST_URL);

        for (Map.Entry<String, String> e : params.entrySet())
            sb.a(e.getKey()).a('=').a(e.getValue()).a('&');

        String qry = sb.toString();

        try {
            URL url = new URL(qry);

            URLConnection conn = url.openConnection();

            conn.setRequestProperty("Accept-Charset", CHARSET);

            InputStream in = conn.getInputStream();

            StringBuilder buf = new StringBuilder(256);

            try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, "UTF-8"))) {
                for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                    buf.append(line);
            }

            return buf.toString();
        }
        catch (IOException e) {
            error("Failed to send HTTP request: " + TEST_URL + "?" + qry, e);

            throw e;
        }
    }

    /**
     * @param content Content to check.
     */
    private JsonNode jsonResponse(String content) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        assertFalse(node.get("affinityNodeId").asText().isEmpty());
        assertEquals(0, node.get("successStatus").asInt());
        assertTrue(node.get("error").isNull());
        assertTrue(node.get("sessionToken").isNull());

        return node.get("response");
    }

    /**
     * Json format string in cache should not transform to Json object on get request.
     */
    @Test
    public void testSkipString2JsonTransformation() throws Exception {
        String val = "{\"v\":\"my Value\",\"t\":1422559650154}";

        // Put to cache JSON format string value.
        String ret = content(F.asMap("cmd", GridRestCommand.CACHE_PUT.key(), "cacheName", LOCAL_CACHE,
            "key", "a", "val", URLEncoder.encode(val, CHARSET)));

        JsonNode res = jsonResponse(ret);

        assertEquals("Incorrect put response", true, res.asBoolean());

        // Escape '\' symbols disappear from response string on transformation to JSON object.
        ret = content(F.asMap("cmd", GridRestCommand.CACHE_GET.key(), "cacheName", LOCAL_CACHE, "key", "a"));

        res = jsonResponse(ret);

        assertEquals("Incorrect get response", val, res.asText());
    }
}
