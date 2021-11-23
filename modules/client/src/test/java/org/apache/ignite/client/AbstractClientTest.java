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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.util.ResourceLeakDetector;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for client tests.
 */
public abstract class AbstractClientTest {
    protected static final String DEFAULT_TABLE = "default_test_table";
    
    protected static TestServer testServer;
    
    protected static Ignite server;
    
    protected static Ignite client;
    
    protected static int serverPort;
    
    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        
        server = new FakeIgnite();
        
        testServer = startServer(10800, 10, server);
        
        serverPort = getPort(testServer.module());
        
        client = startClient();
    }
    
    /**
     * After all.
     */
    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        testServer.close();
    }
    
    /**
     * After each.
     */
    @BeforeEach
    public void beforeEach() {
        for (var t : server.tables().tables()) {
            server.tables().dropTable(t.name());
        }
    }
    
    /**
     * Returns client.
     *
     * @param addrs Addresses.
     * @return Client.
     */
    public static Ignite startClient(String... addrs) {
        if (addrs == null || addrs.length == 0) {
            addrs = new String[]{"127.0.0.1:" + serverPort};
        }
        
        var builder = IgniteClient.builder().addresses(addrs);
        
        return builder.build();
    }
    
    /**
     * Returns server.
     *
     * @param port Port.
     * @param portRange Port range.
     * @param ignite Ignite.
     * @return Server.
     */
    public static TestServer startServer(
            int port,
            int portRange,
            Ignite ignite
    ) {
        return new TestServer(port, portRange, ignite);
    }
    
    /**
     * Assertion of {@link Tuple} equality.
     *
     * @param x Tuple.
     * @param y Tuple.
     */
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
