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

package org.apache.ignite.internal.processors.odbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.events.ConnectionClosedEvent;
import org.apache.ignite.client.events.ConnectionEventListener;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/** */
public class ClientSessionOutboundQueueLimitTest extends GridCommonAbstractTest {
    /** */
    public static final int MSG_QUEUE_LIMIT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSessionOutboundMessageQueueLimit(MSG_QUEUE_LIMIT));
    }

    /**
     * Test scenario:
     * 1. Thin client performs huge amount of async cache get requests. It does not matter if they belong to unique keys.
     * 2. Server accepts all of them and prepares responses that are accumulated in the thin client session outbound
     *    message queue.
     * 3. Programmatically, we limit the thin client's ability to receive messages from the server, causing the message
     *    queue to eventually become full.
     * 4. Checks that thin client is disconnected from the cluster and all requests in progress are failed.
     */
    @Test
    public void testClientSessionOutboundQueueLimit() throws Exception {
        startGrid(0);

        AtomicBoolean isCliDisconnected = new AtomicBoolean(false);

        try (
            IgniteClient cli = Ignition.startClient(new ClientConfiguration()
                .setAddresses("127.0.0.1:10800")
                .setTimeout(5000) // Server will drop packets intended for the client. So client can hang on handshake during reconnect.
                .setRetryLimit(1) // Let's not retry operations if the channel was closed while waiting for a response.
                .setEventListeners(new ConnectionEventListener() {
                    @Override public void onConnectionClosed(ConnectionClosedEvent event) {
                        isCliDisconnected.set(true);
                    }
                }))
        ) {
            ClientCache<Integer, byte[]> cache = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

            byte[] val = new byte[4096];

            ThreadLocalRandom.current().nextBytes(val);

            cache.put(0, val);

            skipClientWrite(grid(0), true);

            Collection<IgniteClientFuture<byte[]>> futs = new ArrayList<>();

            try {
                while (!isCliDisconnected.get()) {
                    futs.add(cache.getAsync(0));

                    // Slow and steady. This delay will give us a chance to stop spamming requests
                    // as soon as server disconnects the client.
                    U.sleep(10);
                }
            }
            finally {
                skipClientWrite(grid(0), false);
            }

            AtomicInteger failedReqsCntr = new AtomicInteger();

            futs.forEach(fut -> {
                try {
                    fut.get();
                }
                catch (Exception e) {
                    failedReqsCntr.incrementAndGet();
                }
            });

            assertTrue(failedReqsCntr.get() >= MSG_QUEUE_LIMIT);
        }
    }

    /** */
    private void skipClientWrite(IgniteEx ignite, boolean skip) {
        ClientListenerProcessor cliPrc = ignite.context().clientListener();

        setFieldValue(U.field(cliPrc, "srv"), "skipWrite", skip);
    }
}
