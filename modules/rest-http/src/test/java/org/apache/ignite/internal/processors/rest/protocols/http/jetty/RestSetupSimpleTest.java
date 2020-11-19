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

import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Integration test for Grid REST functionality; Jetty is under the hood.
 */
public class RestSetupSimpleTest extends GridCommonAbstractTest {
    /** Jetty port. */
    private static final int JETTY_PORT = 8080;

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
        URLConnection conn = new URL("http://localhost:" + JETTY_PORT + "/ignite?cmd=version").openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> myMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info("Version command response is: " + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));
        }
    }
}
