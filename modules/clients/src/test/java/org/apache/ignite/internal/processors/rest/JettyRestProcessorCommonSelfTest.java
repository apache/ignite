/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;

/**
 * Base class for testing Jetty REST protocol.
 */
public abstract class JettyRestProcessorCommonSelfTest extends AbstractRestProcessorSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** REST port. */
    private static final int DFLT_REST_PORT = 8091;

    /** JSON to java mapper. */
    protected static final ObjectMapper JSON_MAPPER = new GridJettyObjectMapper();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, Integer.toString(restPort()));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Port to use for rest. Needs to be changed over time because Jetty has some delay before port unbind.
     */
    protected int restPort() {
        return DFLT_REST_PORT;
    }

    /**
     * @return Test URL
     */
    protected String restUrl() {
        return "http://" + LOC_HOST + ":" + restPort() + "/ignite?";
    }

    /**
     * @return Security enabled flag. Should be the same with {@code ctx.security().enabled()}.
     */
    protected boolean securityEnabled() {
        return false;
    }

    /**
     * @return Signature.
     * @throws Exception If failed.
     */
    protected abstract String signature() throws Exception;

    /**
     * Execute REST command and return result.
     *
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    protected String content(Map<String, String> params) throws Exception {
        SB sb = new SB(restUrl());

        for (Map.Entry<String, String> e : params.entrySet())
            sb.a(e.getKey()).a('=').a(e.getValue()).a('&');

        URL url = new URL(sb.toString());

        URLConnection conn = url.openConnection();

        String signature = signature();

        if (signature != null)
            conn.setRequestProperty("X-Signature", signature);

        InputStream in = conn.getInputStream();

        StringBuilder buf = new StringBuilder(256);

        try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, "UTF-8"))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                buf.append(line);
        }

        return buf.toString();
    }

    /**
     * @param cacheName Optional cache name.
     * @param cmd REST command.
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    protected String content(String cacheName, GridRestCommand cmd, String... params) throws Exception {
        Map<String, String> paramsMap = new LinkedHashMap<>();

        if (cacheName != null)
            paramsMap.put("cacheName", cacheName);

        paramsMap.put("cmd", cmd.key());

        if (params != null) {
            assertEquals(0, params.length % 2);

            for (int i = 0; i < params.length; i += 2)
                paramsMap.put(params[i], params[i + 1]);
        }

        return content(paramsMap);
    }

    /**
     * @param json JSON content.
     * @param field Field name in JSON object.
     * @return Field value.
     * @throws IOException If failed.
     */
    protected String jsonField(String json, String field) throws IOException {
        assertNotNull(json);
        assertFalse(json.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(json);

        JsonNode fld = node.get(field);

        assertNotNull(fld);

        return fld.asText();
    }
}
