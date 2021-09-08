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

package org.apache.ignite.internal.processors.security.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class AdditionalSecurityCheckTest extends CommonSecurityCheckTest {
    /**
     *
     */
    @Test
    public void testClientInfo() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());
        assertFalse(ignite.cluster().active());

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertTrue(client.connected());

            client.state().state(ACTIVE, false);
        }

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            client.createCache("test_cache");

            assertEquals(1, client.cacheNames().size());
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoGridClientFail() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());

        fail = true;

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertFalse(client.connected());
            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    throw client.checkLastError();
                },
                GridClientAuthenticationException.class,
                "Client version is not found.");
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoIgniteClientFail() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());

        fail = true;

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            fail();
        }
        catch (ClientAuthenticationException e) {
            assertTrue(e.getMessage().contains("Client version is not found"));
        }
    }

    /**
     *
     */
    @Test
    public void testClientInfoClientFail() throws Exception {
        Ignite ignite = startGrids(1);

        assertEquals(1, ignite.cluster().topologyVersion());

        fail = true;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                startGrid(2);
                return null;
            },
            IgniteSpiException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
    }

    /**
     *
     */
    @Test
    public void testAdditionalPasswordServerFail() throws Exception {
        Ignite ignite = startGrid(0);

        fail = true;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                startGrid(1);
                return null;
            },
            IgniteAuthenticationException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
    }
}
