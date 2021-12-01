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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletionException;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client reconnect.
 */
public class ReconnectTest {
    /** Test server. */
    TestServer server;

    /** Test server 2. */
    TestServer server2;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server, server2);
    }

    @Test
    public void clientReconnectsToAnotherAddressOnNodeFail() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ignite1.tables().createTable("t", c -> c.changeName("t"));

        server = AbstractClientTest.startServer(
                10900,
                10,
                ignite1);

        var client = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                .retryLimit(100)
                .build();

        assertEquals("t", client.tables().tables().get(0).name());

        server.close();

        FakeIgnite ignite2 = new FakeIgnite();
        ignite2.tables().createTable("t2", c -> c.changeName("t2"));

        server2 = AbstractClientTest.startServer(
                10950,
                10,
                ignite2);

        assertEquals("t2", client.tables().tables().get(0).name());
    }

    @Test
    public void testOperationFailsWhenAllServersFail() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ignite1.tables().createTable("t", c -> c.changeName("t"));

        server = AbstractClientTest.startServer(
                10900,
                10,
                ignite1);

        var client = IgniteClient.builder()
                .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                .retryLimit(100)
                .build();

        assertEquals("t", client.tables().tables().get(0).name());

        server.close();

        var ex = assertThrows(CompletionException.class, () -> client.tables().tables());
        assertEquals(ex.getCause().getMessage(), "Channel is closed");
    }
}
