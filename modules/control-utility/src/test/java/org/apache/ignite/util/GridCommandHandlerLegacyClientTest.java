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

import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/** */
public class GridCommandHandlerLegacyClientTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testDefaultConfiguration() throws Exception {
        startGrid();

        checkExecute("Cluster state: ACTIVE", "--state");
    }

    /** */
    @Test
    public void testNonDefaultPort() throws Exception {
        int port = 11212;

        startGrid(getConfiguration()
            .setConnectorConfiguration(new ConnectorConfiguration()
                .setPort(port)));

        checkExecute("Make sure you are connecting to the client connector", "--port", String.valueOf(port), "--state");

        System.setProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION, "true");

        try {
            checkExecute("Cluster state: ACTIVE", "--port", String.valueOf(port), "--state");
        }
        finally {
            System.clearProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION);
        }

        checkExecute("Cluster state: ACTIVE", "--state");
    }

    /** */
    @Test
    public void testCustomSsl() throws Exception {
        startGrid(getConfiguration()
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setUseIgniteSslContextFactory(false)
                .setSslContextFactory(sslTrustedFactory("thinServer", "trusttwo")))
            .setConnectorConfiguration(new ConnectorConfiguration()
                .setSslEnabled(true)
                .setSslFactory(sslTrustedFactory("connectorServer", "trustthree"))));

        checkExecute("Make sure you are connecting to the client connector",
            "--keystore", GridTestUtils.keyStorePath("connectorClient"),
            "--keystore-password", GridTestUtils.keyStorePassword(),
            "--truststore", keyStorePath("trustthree"),
            "--truststore-password", keyStorePassword(),
            "--state");

        System.setProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION, "true");

        try {
            checkExecute("Cluster state: ACTIVE",
                "--keystore", GridTestUtils.keyStorePath("connectorClient"),
                "--keystore-password", GridTestUtils.keyStorePassword(),
                "--truststore", keyStorePath("trustthree"),
                "--truststore-password", keyStorePassword(),
                "--state");
        }
        finally {
            System.clearProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION);
        }

        checkExecute("Cluster state: ACTIVE",
            "--keystore", GridTestUtils.keyStorePath("thinClient"),
            "--keystore-password", GridTestUtils.keyStorePassword(),
            "--truststore", keyStorePath("trusttwo"),
            "--truststore-password", keyStorePassword(),
            "--state");
    }

    /** */
    @Test
    public void testClientConnectorDisabled() throws Exception {
        startGrid(getConfiguration()
            .setClientConnectorConfiguration(null)
            .setConnectorConfiguration(new ConnectorConfiguration()));

        checkExecute("Make sure you are connecting to the client connector", "--state");

        System.setProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION, "true");

        try {
            checkExecute("Cluster state: ACTIVE", "--state");
        }
        finally {
            System.clearProperty(IGNITE_CONTROL_UTILITY_USE_CONNECTOR_CONNECTION);
        }
    }

    /** */
    private void checkExecute(String expMsg, String... params) {
        ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

        LogListener lsnr = LogListener.matches(expMsg).build();

        log.registerListener(lsnr);

        new CommandHandler(log).execute(F.asList(params));

        assertTrue(lsnr.check());
    }
}
