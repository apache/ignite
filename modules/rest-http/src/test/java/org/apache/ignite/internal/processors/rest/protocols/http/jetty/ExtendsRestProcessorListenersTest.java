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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests extends REST processor.
 */
public class ExtendsRestProcessorListenersTest extends GridCommonAbstractTest {
    /** */
    private final Map<GridRestRequest, IgniteInternalFuture<GridRestResponse>> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        TestRestProcessorPluginProvider restProcPluginProvider = new TestRestProcessorPluginProvider();

        restProcPluginProvider.setConfiguration(new TestRestProcessorPluginConfiguration().setListener(map::put));

        return super.getConfiguration(instanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setPluginProviders(restProcPluginProvider);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void restListenerTest() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        assertEquals(ignite.context().rest().getClass(), TestGridRestProcessor.class);

        TestGridRestProcessor rest = (TestGridRestProcessor)ignite.context().rest();

        assertNotNull(rest.getListener());

        assertTrue(map.isEmpty());

        executeCommand(GridRestCommand.VERSION);

        assertFalse(map.isEmpty());

        Map.Entry<GridRestRequest, IgniteInternalFuture<GridRestResponse>> entry = map.entrySet().iterator().next();

        assertEquals(GridRestCommand.VERSION, entry.getKey().command());

        assertEquals(IgniteVersionUtils.VER_STR, entry.getValue().get().getResponse());
    }

    /** */
    private void executeCommand(GridRestCommand cmd) throws IOException {
        String addr = "http://127.0.0.1:8080/ignite?cmd=" + cmd.key();

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }
}
