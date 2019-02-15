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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests DataStreamer reconnect behaviour when client nodes arrives at the same or different topVer than it left.
 */
@RunWith(JUnit4.class)
public class DataStreamerClientReconnectAfterClusterRestartTest extends GridCommonAbstractTest {
    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>("test"));

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** */
    @Test
    public void testOneClient() throws Exception {
        clusterRestart(false, false);
    }

    /** */
    @Test
    public void testOneClientAllowOverwrite() throws Exception {
        clusterRestart(false, true);
    }

    /** */
    @Test
    public void testTwoClients() throws Exception {
        clusterRestart(true, false);
    }

    /** */
    @Test
    public void testTwoClientsAllowOverwrite() throws Exception {
        clusterRestart(true, true);
    }

    /** */
    private void clusterRestart(boolean withAnotherClient, boolean allowOverwrite) throws Exception {
        CountDownLatch disconnect = new CountDownLatch(1);
        CountDownLatch reconnect = new CountDownLatch(1);

        try {
            startGrid(0);

            clientMode = true;

            Ignite client = startGrid(1);

            if (withAnotherClient) {
                // Force increase of topVer
                startGrid(2);

                stopGrid(2);
            }

            clientMode = false;

            try (IgniteDataStreamer<String, String> streamer = client.dataStreamer("test")) {
                streamer.allowOverwrite(allowOverwrite);

                streamer.addData("k1", "v1");
            }

            // Restart the cluster so that client reconnects to a new one
            client.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event event) {
                    reconnect.countDown();

                    return false;
                }
            }, EventType.EVT_CLIENT_NODE_RECONNECTED);

            client.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event event) {
                    disconnect.countDown();

                    return false;
                }
            }, EventType.EVT_CLIENT_NODE_DISCONNECTED);

            stopGrid(0);

            disconnect.await();

            startGrid(0);

            reconnect.await();

            try (IgniteDataStreamer<String, String> streamer = client.dataStreamer("test")) {
                streamer.allowOverwrite(allowOverwrite);

                streamer.addData("k2", "v2");

                return;
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
