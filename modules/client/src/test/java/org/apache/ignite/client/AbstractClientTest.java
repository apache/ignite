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
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
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

    protected static ClientHandlerModule serverModule;

    protected static Ignite server;

    protected static Ignite client;

    protected static int serverPort;

    @BeforeAll
    public static void beforeAll() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        serverModule = startServer();
        serverPort = ((InetSocketAddress) Objects.requireNonNull(serverModule.localAddress())).getPort();

        client = startClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        serverModule.stop();
        configurationRegistry.stop();
    }

    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables())
            server.tables().dropTable(t.tableName());
    }

    public static Ignite startClient(String... addrs) {
        if (addrs == null || addrs.length == 0)
            addrs = new String[]{"127.0.0.2:" + serverPort};

        var builder = IgniteClient.builder().addresses(addrs);

        return builder.build();
    }

    public static ClientHandlerModule startServer() {
        configurationRegistry = new ConfigurationRegistry(
            List.of(ClientConnectorConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL)
        );

        configurationRegistry.start();

        configurationRegistry.getConfiguration(ClientConnectorConfiguration.KEY).change(
                local -> local.changePort(10800).changePortRange(10)
        ).join();

        server = new FakeIgnite();

        var module = new ClientHandlerModule(server.tables(), configurationRegistry);
        module.start();

        return module;
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

        assertEquals(x.columnCount(), y.columnCount());

        for (var i = 0; i < x.columnCount(); i++) {
            assertEquals(x.columnName(i), y.columnName(i));
            assertEquals((Object) x.value(i), y.value(i));
        }
    }
}
