/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

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
    @Test
    public void testStopWhenDisconnected() throws Exception {
        clientMode = true;

        Ignite client = startGrid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        DiscoverySpi srvSpi = spi0(srv);
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final IgniteDiscoverySpi clientSpi = spi0(client);

        DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

        clientSpi.setInternalListener(lsnr);

        log.info("Block reconnect.");

        lsnr.startBlockJoin();

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
            client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

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
