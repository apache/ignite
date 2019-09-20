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

package org.apache.ignite.internal.processors.rest;

import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;

/** */
public class RestProcessedCommandListenerTest extends GridCommonAbstractTest {
    /** Node local host. */
    private static final String HOST = "127.0.0.1";

    /** Binary rest port. */
    private static final int BINARY_PORT = 11212;

    /** */
    private static final Queue<GridRestRequest> LISTENED_CMDS = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setActiveOnStart(false)
            .setAutoActivationEnabled(false)
            .setConnectorConfiguration(
                new ConnectorConfiguration()
                    .setHost(HOST)
                    .setPort(BINARY_PORT)
            );
    }

    /** */
    @Test
    public void testRestCommandListener() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.context().rest().listenProcessedCommands((req, res) -> !LISTENED_CMDS.add(req), CLUSTER_ACTIVATE);

        GridClient client = startClient();

        checkListenedCommands(() -> performRemoteActivation(client), CLUSTER_ACTIVATE);

        ignite.cluster().active(false);

        checkListenedCommands(() -> performRemoteActivation(client));
    }

    /** */
    private void performRemoteActivation(GridClient client) {
        try {
            client.state().active(true);
        }
        catch (GridClientException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void checkListenedCommands(Runnable r, GridRestCommand... cmds) throws Exception {
        clearListenedRequests();

        r.run();

        GridTestUtils.waitForCondition(() ->
            Arrays.stream(cmds)
                .allMatch(cmd ->
                    LISTENED_CMDS.stream()
                        .map(GridRestRequest::command)
                        .collect(Collectors.toList())
                        .contains(cmd)),
            getTestTimeout()
        );

        assertTrue(Arrays.asList(cmds).size() == LISTENED_CMDS.size());
    }

    /** */
    private void clearListenedRequests() {
        LISTENED_CMDS.clear();
    }

    /** */
    protected GridClient startClient() throws GridClientException {
        return GridClientFactory.start(new GridClientConfiguration()
            .setConnectTimeout(300)
            .setServers(Collections
                .singleton(HOST + ":" + BINARY_PORT)));
    }
}
