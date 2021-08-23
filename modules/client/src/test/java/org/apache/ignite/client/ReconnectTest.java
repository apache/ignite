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

import java.util.concurrent.CompletionException;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests thin client reconnect.
 */
public class ReconnectTest {
    @Test
    public void clientReconnectsToAnotherAddressOnNodeFail() throws Exception {
        IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> srv = null;
        IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> srv2 = null;

        try {
            FakeIgnite ignite1 = new FakeIgnite();
            ignite1.tables().createTable("t", c -> c.changeName("t"));

            srv = AbstractClientTest.startServer(
                    10900,
                    10,
                    ignite1);

            var client = IgniteClient.builder()
                    .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                    .retryLimit(100)
                    .build();

            assertEquals("t", client.tables().tables().get(0).tableName());

            stop(srv);

            FakeIgnite ignite2 = new FakeIgnite();
            ignite2.tables().createTable("t2", c -> c.changeName("t2"));

            srv2 = AbstractClientTest.startServer(
                    10950,
                    10,
                    ignite2);

            assertEquals("t2", client.tables().tables().get(0).tableName());
        } finally {
            stop(srv);
            stop(srv2);
        }
    }

    @Test
    public void testOperationFailsWhenAllServersFail() throws Exception {
        IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> srv = null;

        try {
            FakeIgnite ignite1 = new FakeIgnite();
            ignite1.tables().createTable("t", c -> c.changeName("t"));

            srv = AbstractClientTest.startServer(
                    10900,
                    10,
                    ignite1);

            var client = IgniteClient.builder()
                    .addresses("127.0.0.1:10900..10910", "127.0.0.1:10950..10960")
                    .retryLimit(100)
                    .build();

            assertEquals("t", client.tables().tables().get(0).tableName());

            stop(srv);

            var ex = assertThrows(CompletionException.class, () -> client.tables().tables());
            assertEquals(ex.getCause().getMessage(), "Channel is closed");
        } finally {
            stop(srv);
        }
    }

    private void stop(IgniteBiTuple<ClientHandlerModule, ConfigurationRegistry> srv) throws Exception {
        if (srv == null)
            return;

        srv.get1().stop();
        srv.get2().stop();
    }
}
