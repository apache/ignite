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

package org.apache.ignite.util;

import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/** Tests cluster-wide TLS reload through the control utility. */
public class GridCommandHandlerSslReloadTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return List.of(CLI_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected SslContextFactory sslFactory() {
        return (SslContextFactory)sslTrustedFactory("node01", "trustboth");
    }

    /** {@inheritDoc} */
    @Override protected void extendSslParams(List<String> params) {
        super.extendSslParams(params);

        params.addAll(List.of("--truststore", keyStorePath("trustboth"), "--truststore-password", keyStorePassword()));
    }

    /** A client node failure leaves its certificate in service and the server reload still completes. */
    @Test
    public void testReloadReportsPartialFailure() throws Exception {
        persistenceEnable(false);
        injectTestSystemOut();

        IgniteEx srv = startGrid(0);
        IgniteEx client = startClientGrid(CLIENT_NODE_NAME_PREFIX, cfg -> cfg.setConnectorConfiguration(null));
        String port = Integer.toString(srv.context().clientListener().port());

        ((SslContextFactory)srv.configuration().getSslContextFactory()).setKeyStoreFilePath(keyStorePath("node02"));
        ((SslContextFactory)client.configuration().getSslContextFactory()).setKeyStoreFilePath(keyStorePath("node02old"));

        String reload = executeCommand(EXIT_CODE_UNEXPECTED_ERROR, "--ssl", "reload", "--port", port);

        assertContains(log, reload, node(client) + ": failed on ");
        assertContains(log, reload, "alias=node02old");
        assertContains(log, reload, node(srv) + ": reloaded ");

        String status = executeCommand(EXIT_CODE_OK, "--ssl", "status", "--port", port);

        assertContains(log, status, node(srv) + ": binary REST, client connector, communication, discovery\n" +
            DOUBLE_INDENT + "loaded alias=node02,");
        assertContains(log, status, node(client) + ": client connector, communication, discovery\n" +
            DOUBLE_INDENT + "loaded alias=node01,");
    }

    /** Returns the node identifier used in command reports. */
    private static String node(IgniteEx node) {
        return node.localNode().id() + " [consistentId=" + node.localNode().consistentId() + ']';
    }
}
