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

package org.apache.ignite.client;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import io.netty.util.ResourceLeakDetector;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Base class for client tests.
 */
public abstract class AbstractClientTest {
    protected static final String DEFAULT_TABLE = "default_test_table";

    protected static ConfigurationRegistry configurationRegistry;

    protected static ClientHandlerModule clientHandlerModule;

    protected static Ignite server;

    protected static Ignite client;

    protected static int serverPort;

    @BeforeAll
    public static void beforeAll() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        server = new FakeIgnite();

        IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> srv = startServer(10800, 10, server);

        clientHandlerModule = srv.get1();
        configurationRegistry = srv.get2();

        serverPort = getPort(clientHandlerModule);

        client = startClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        clientHandlerModule.stop();
        configurationRegistry.stop();
    }

    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables())
            server.tables().dropTable(t.tableName());
    }

    public static Ignite startClient(String... addrs) {
        if (addrs == null || addrs.length == 0)
            addrs = new String[]{"127.0.0.1:" + serverPort};

        var builder = IgniteClient.builder().addresses(addrs);

        return builder.build();
    }

    public static IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> startServer(
            int port,
            int portRange,
            Ignite ignite
    ) {
        var cfg = new ConfigurationRegistry(
            List.of(ClientConnectorConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of()
        );

        cfg.start();

        cfg.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local.changePort(port).changePortRange(portRange)
        ).join();

        var module = new ClientHandlerModule(((FakeIgnite)ignite).queryEngine(), ignite.tables(), cfg);
        module.start();

        return new IgniteBiTuple<>(module, cfg);
    }

    public static void assertTupleEquals(Tuple x, Tuple y) {
        if (x == null) {
            assertNull(y);
            return;
        }

        if (y == null) {
            //noinspection ConstantConditions
            assertNull(x);
            return;
        }

        assertEquals(x.columnCount(), y.columnCount(), x + " != " + y);

        for (var i = 0; i < x.columnCount(); i++) {
            assertEquals(x.columnName(i), y.columnName(i));
            assertEquals((Object) x.value(i), y.value(i));
        }
    }

    public static int getPort(ClientHandlerModule hnd) {
        return ((InetSocketAddress) Objects.requireNonNull(hnd.localAddress())).getPort();
    }
}
