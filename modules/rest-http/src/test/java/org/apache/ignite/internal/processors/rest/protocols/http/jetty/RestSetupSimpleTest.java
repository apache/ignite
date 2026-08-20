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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.DFLT_JETTY_PORT;

/**
 * Integration test for Grid REST functionality; Jetty is under the hood.
 */
public class RestSetupSimpleTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * Runs version command using GridJettyRestProtocol.
     */
    @Test
    public void testVersionCommand() throws Exception {
        startGrid();

        String res = execute("/ignite", new T2<>("cmd", "version"));

        Map<String, Object> val = new ObjectMapper().readValue(res, new TypeReference<>() {});

        log.info("Version command response is: " + val);

        assertTrue(val.containsKey("response"));
        assertEquals(0, val.get("successStatus"));
    }

    /** */
    @SafeVarargs
    public static String execute(String path, T2<String, String>... params) throws Exception {
        return execute(SC_OK, path, params);
    }

    /** */
    @SafeVarargs
    public static String execute(int expCode, String path, T2<String, String>... params) throws Exception {
        GridStringBuilder url = new GridStringBuilder("http://localhost:" + DFLT_JETTY_PORT + path + "?");

        for (T2<String, String> p : params)
            url.a(p.get1()).a("=").a(p.get2()).a("&");

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .build();

        HttpResponse<String> res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(expCode, res.statusCode());

        return res.body();
    }
}
