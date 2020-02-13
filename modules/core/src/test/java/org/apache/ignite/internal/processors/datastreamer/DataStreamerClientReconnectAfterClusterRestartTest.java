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

/**
 * Tests DataStreamer reconnect behaviour when client nodes arrives at the same or different topVer than it left.
 */
public class DataStreamerClientReconnectAfterClusterRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>("test"));

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

            Ignite client = startClientGrid(1);

            if (withAnotherClient) {
                // Force increase of topVer
                startClientGrid(2);

                stopGrid(2);
            }

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
