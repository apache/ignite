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
import java.util.Collections;
import io.netty.channel.ChannelFuture;
import io.netty.util.ResourceLeakDetector;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.table.ClientTupleBuilder;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.helpers.NOPLogger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Base class for client tests.
 */
public abstract class AbstractClientTest {
    protected static final String DEFAULT_TABLE = "default_test_table";

    protected static ConfigurationRegistry configurationRegistry;

    protected static ChannelFuture serverFuture;

    protected static Ignite server;

    protected static Ignite client;

    @BeforeAll
    public static void beforeAll() throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        serverFuture = startServer(null);
        client = startClient();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        serverFuture.cancel(true);
        serverFuture.await();
        configurationRegistry.stop();
    }

    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables())
            server.tables().dropTable(t.tableName());
    }

    public static Ignite startClient(String... addrs) {
        if (addrs == null || addrs.length == 0) {
            var serverPort = ((InetSocketAddress)serverFuture.channel().localAddress()).getPort();

            addrs = new String[]{"127.0.0.2:" + serverPort};
        }

        var builder = IgniteClient.builder().addresses(addrs);

        return builder.build();
    }

    public static ChannelFuture startServer(String host) throws InterruptedException {
        configurationRegistry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        configurationRegistry.start();

        server = new FakeIgnite();

        var module = new ClientHandlerModule(server, NOPLogger.NOP_LOGGER);

        module.prepareStart(configurationRegistry);

        return module.start();
    }

    public static void assertTupleEquals(Tuple x, Tuple y) {
        if (x == null)
            assertNull(y);

        if (y == null)
            assertNull(x);

        var a = (ClientTupleBuilder) x;
        var b = (ClientTupleBuilder) y;

        assertEquals(a.columnCount(), b.columnCount());

        for (var i = 0; i < a.columnCount(); i++) {
            assertEquals(a.columnName(i), b.columnName(i));
            assertEquals((Object)a.value(i), b.value(i));
        }
    }
}
