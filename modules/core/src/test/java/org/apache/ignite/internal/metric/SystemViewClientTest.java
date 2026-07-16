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

package org.apache.ignite.internal.metric;

import java.sql.Connection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.systemview.ClientConnectionAttributeViewWalker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.ClientConnectionAttributeView;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_ATTR_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.util.lang.GridFunc.identity;

/** Tests for {@link SystemView} for clients. */
public class SystemViewClientTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testClientsConnections() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            String host = g0.configuration().getClientConnectorConfiguration().getHost();

            if (host == null)
                host = g0.configuration().getLocalHost();

            int port = g0.configuration().getClientConnectorConfiguration().getPort();

            SystemView<ClientConnectionView> conns = g0.context().systemView().view(CLI_CONN_VIEW);

            try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
                assertEquals(1, conns.size());

                ClientConnectionView cliConn = conns.iterator().next();

                assertEquals("THIN", cliConn.type());
                assertEquals(cliConn.localAddress().getHostName(), cliConn.remoteAddress().getHostName());
                assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                    cliConn.localAddress().getPort());
                assertEquals(cliConn.version(), ProtocolVersion.LATEST_VER.toString());

                try (Connection conn =
                         new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                    assertEquals(2, conns.size());
                    assertEquals(1, F.size(jdbcConnectionsIterator(conns)));

                    ClientConnectionView jdbcConn = jdbcConnectionsIterator(conns).next();

                    assertEquals("JDBC", jdbcConn.type());
                    assertEquals(jdbcConn.localAddress().getHostName(), jdbcConn.remoteAddress().getHostName());
                    assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                        jdbcConn.localAddress().getPort());
                    assertEquals(jdbcConn.version(), JdbcConnectionContext.CURRENT_VER.asString());
                }
            }

            boolean res = GridTestUtils.waitForCondition(() -> conns.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DATA_CENTER_ID, value = "server-dc")
    public void testClientConnectionDataCenterId() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            assertEquals("server-dc", g0.localNode().dataCenterId());

            System.clearProperty(IGNITE_DATA_CENTER_ID);

            SystemView<ClientConnectionView> conns = g0.context().systemView().view(CLI_CONN_VIEW);

            Map<String, String> userAttrs = Collections.singletonMap(IGNITE_DATA_CENTER_ID, "user-dc");

            try (IgniteClient ignored = Ignition.startClient(
                new ClientConfiguration()
                    .setAddresses(Config.SERVER)
                    .setUserAttributes(userAttrs))) {
                assertEquals("user-dc", conns.iterator().next().dataCenterId());
            }

            assertTrue(GridTestUtils.waitForCondition(() -> conns.size() == 0, 5_000));

            System.setProperty(IGNITE_DATA_CENTER_ID, "property-dc");

            try (IgniteClient ignored = Ignition.startClient(
                new ClientConfiguration()
                    .setAddresses(Config.SERVER)
                    .setUserAttributes(userAttrs))) {
                System.clearProperty(IGNITE_DATA_CENTER_ID);

                assertEquals("property-dc", conns.iterator().next().dataCenterId());
                assertEquals("user-dc", userAttrs.get(IGNITE_DATA_CENTER_ID));
            }

            assertTrue(GridTestUtils.waitForCondition(() -> conns.size() == 0, 5_000));

            try (IgniteClient ignored = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                assertNull(conns.iterator().next().dataCenterId());
            }
        }
    }

    /** */
    @Test
    public void testClientConnectionAttributes() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            SystemView<ClientConnectionAttributeView> view = g0.context().systemView().view(CLI_CONN_ATTR_VIEW);

            try (
                IgniteClient cl1 = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                    .setUserAttributes(F.asMap("attr1", "val1", "attr2", "val2")));
                IgniteClient cl2 = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                    .setUserAttributes(F.asMap("attr1", "val2")));
                IgniteClient cl3 = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
            ) {
                assertEquals(3, F.size(view.iterator()));

                assertEquals(1, F.size(view.iterator(), row ->
                    "attr1".equals(row.name()) && "val1".equals(row.value())));

                // Test filtering.
                assertTrue(view instanceof FiltrableSystemView);

                Iterator<ClientConnectionAttributeView> iter = ((FiltrableSystemView<ClientConnectionAttributeView>)view)
                    .iterator(F.asMap(ClientConnectionAttributeViewWalker.NAME_FILTER, "attr1"));

                assertEquals(2, F.size(iter));

                iter = ((FiltrableSystemView<ClientConnectionAttributeView>)view).iterator(
                    F.asMap(ClientConnectionAttributeViewWalker.NAME_FILTER, "attr2"));

                assertTrue(iter.hasNext());

                long connId = iter.next().connectionId();

                assertFalse(iter.hasNext());

                iter = ((FiltrableSystemView<ClientConnectionAttributeView>)view).iterator(
                    F.asMap(ClientConnectionAttributeViewWalker.CONNECTION_ID_FILTER, connId));

                assertEquals(2, F.size(iter));
            }
        }
    }

    /** */
    private Iterator<ClientConnectionView> jdbcConnectionsIterator(SystemView<ClientConnectionView> conns) {
        return F.iterator(conns.iterator(), identity(), true, v -> "JDBC".equals(v.type()));
    }
}
