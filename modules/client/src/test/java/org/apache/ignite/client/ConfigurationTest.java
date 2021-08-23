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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests thin client configuration.
 */
public class ConfigurationTest extends AbstractClientTest {
    @Test
    public void testClientValidatesAddresses() {
        IgniteClient.Builder builder = IgniteClient.builder();

        var ex = assertThrows(IgniteClientException.class, builder::build);

        assertEquals("Empty addresses", ex.getMessage());
    }

    @Test
    public void testClientValidatesPorts() {
        IgniteClient.Builder builder = IgniteClient.builder()
                .addresses("127.0.0.1:70000");

        var ex = assertThrows(IgniteException.class, builder::build);

        assertEquals(
                "Failed to parse Ignite server address (port range contains invalid port 70000): 127.0.0.1:70000",
                ex.getMessage());
    }

    @Test
    public void testAddressFinderWorksWithoutAddresses() throws Exception {
        String addr = "127.0.0.1:" + serverPort;

        IgniteClient.Builder builder = IgniteClient.builder();

        IgniteClient client = builder
                .addressFinder(() -> new String[] {addr})
                .build();

        try (client) {
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }
    }

    @Test
    public void testClientBuilderPropagatesAllConfigurationValues() throws Exception {
        String addr = "127.0.0.1:" + serverPort;

        IgniteClient.Builder builder = IgniteClient.builder();

        IgniteClient client = builder
                .addresses(addr)
                .connectTimeout(1234)
                .retryLimit(7)
                .reconnectThrottlingPeriod(123)
                .reconnectThrottlingRetries(8)
                .addressFinder(() -> new String[] {addr})
                .build();

        // Builder can be reused and it won't affect already created clients.
        IgniteClient client2 = builder
                .connectTimeout(2345)
                .retryLimit(8)
                .reconnectThrottlingPeriod(1234)
                .reconnectThrottlingRetries(88)
                .build();

        try (client) {
            // Check that client works.
            assertEquals(0, client.tables().tables().size());

            // Check config values.
            assertEquals("thin-client", client.name());
            assertEquals(1234, client.configuration().connectTimeout());
            assertEquals(7, client.configuration().retryLimit());
            assertEquals(123, client.configuration().reconnectThrottlingPeriod());
            assertEquals(8, client.configuration().reconnectThrottlingRetries());
            assertArrayEquals(new String[]{addr}, client.configuration().addresses());
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }

        try (client2) {
            // Check that client works.
            assertEquals(0, client2.tables().tables().size());

            // Check config values.
            assertEquals(2345, client2.configuration().connectTimeout());
            assertEquals(8, client2.configuration().retryLimit());
            assertEquals(1234, client2.configuration().reconnectThrottlingPeriod());
            assertEquals(88, client2.configuration().reconnectThrottlingRetries());
            assertArrayEquals(new String[]{addr}, client.configuration().addresses());
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }
    }

    @Test
    public void testClientBuilderFailsOnExceptionInAddressFinder() {
        IgniteClient.Builder builder = IgniteClient.builder()
                .addressFinder(() -> { throw new IllegalArgumentException("bad finder"); });

        assertThrows(IllegalArgumentException.class, builder::build);
    }
}
