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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;

import java.util.concurrent.*;

import static org.apache.ignite.events.EventType.*;

/**
 *
 */
public class IgniteClientReconnectStopTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopWhenDisconnected() throws Exception {
        clientMode = true;

        Ignite client = startGrid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final TestTcpDiscoverySpi clientSpi = spi(client);

        log.info("Block reconnect.");

        clientSpi.writeLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                } else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(disconnectLatch);

        IgniteFuture<?> reconnectFut = null;

        try {
            client.getOrCreateCache(new CacheConfiguration<>());

            fail();
        }
        catch (IgniteClientDisconnectedException e) {
            log.info("Expected operation exception: " + e);

            reconnectFut = e.reconnectFuture();
        }

        assertNotNull(reconnectFut);

        client.close();

        try {
            reconnectFut.get();

            fail();
        }
        catch (IgniteException e) {
            log.info("Expected reconnect exception: " + e);
        }
    }
}
