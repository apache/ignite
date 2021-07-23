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

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests client connection to various addresses.
 */
public class ConnectionTest extends AbstractClientTest {
    @Test
    public void testEmptyNodeAddress() {
        var ex = assertThrows(IgniteException.class, () -> testConnection(""));
        assertEquals("Failed to parse Ignite server address (Address is empty): ", ex.getMessage());
    }

    @Test
    public void testNullNodeAddresses() {
        var ex = assertThrows(IgniteException.class, () -> testConnection(null, null));
        assertEquals("Failed to parse Ignite server address (Address is empty): null", ex.getMessage());
    }

    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection("127.0.0.1:10800");
    }

    @Test
    public void testInvalidNodeAddresses() throws Exception {
        var ex = assertThrows(IgniteClientConnectionException.class,
                () -> testConnection("127.0.0.1:47500"));

        assertEquals("Connection refused: /127.0.0.1:47500", ex.getCause().getMessage());
    }

    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801", "127.0.0.1:10800");
    }

    @Disabled("IPv6 is not enabled by default on some systems.")
    @Test
    public void testIPv6NodeAddresses() throws Exception {
        testConnection("[::1]:10800");
    }

    private void testConnection(String... addrs) throws Exception {
        AbstractClientTest.startClient(addrs).close();
    }
}
