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

package org.apache.ignite.internal.client.thin.events;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.events.ClientFailEvent;
import org.apache.ignite.client.events.ClientLifecycleEvent;
import org.apache.ignite.client.events.ClientLifecycleEventListener;
import org.apache.ignite.client.events.ClientStartEvent;
import org.apache.ignite.client.events.ClientStopEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.junit.Test;

/**
 * Tests lifecycle event listeners of a thin client.
 */
public class IgniteClientLifecycleEventListenerTest extends AbstractThinClientTest {
    /** */
    List<ClientLifecycleEvent> evts = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration()
            .setEventListeners(new ClientLifecycleEventListener() {
                @Override public void onClientStart(ClientStartEvent evt) {
                    evts.add(evt);
                }

                @Override public void onClientFail(ClientFailEvent evt) {
                    evts.add(evt);
                }

                @Override public void onClientStop(ClientStopEvent evt) {
                    evts.add(evt);
                }
            });
    }

    /** */
    @Test
    public void testClientLifecycleEvents() throws Exception {
        evts.clear();

        startGrids(3);

        IgniteClient client0;

        try (IgniteClient client = startClient(0, 1, 2)) {
            client0 = client;

            assertEquals(1, evts.size());
            ClientLifecycleEvent evt0 = evts.get(0);
            assertTrue(evt0 instanceof ClientStartEvent);
            assertEquals(client, ((ClientStartEvent)evt0).client());
            assertEquals(3, ((ClientStartEvent)evt0).configuration().getAddresses().length);
        }

        assertEquals(2, evts.size());
        ClientLifecycleEvent evt1 = evts.get(1);
        assertTrue(evt1 instanceof ClientStopEvent);
        assertEquals(client0, ((ClientStopEvent)evt1).client());

        try {
            Ignition.startClient(getClientConfiguration().setAddresses("failure"));
            fail();
        }
        catch (Exception e) {
            assertEquals(3, evts.size());
            ClientLifecycleEvent evt2 = evts.get(2);
            assertTrue(evt2 instanceof ClientFailEvent);
            assertEquals(1, ((ClientFailEvent)evt2).configuration().getAddresses().length);
            assertEquals(e, ((ClientFailEvent)evt2).throwable());
        }
    }
}
