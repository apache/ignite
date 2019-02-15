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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 * Fail fast test.
 */
@RunWith(JUnit4.class)
public class GridFailFastNodeFailureDetectionSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        // Set parameters for fast ping failure.
        disco.setSocketTimeout(100);
        disco.setNetworkTimeout(100);
        disco.setReconnectCount(2);

        cfg.setMetricsUpdateFrequency(10_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailFast() throws Exception {
        startGridsMultiThreaded(5);

        final CountDownLatch failLatch = new CountDownLatch(4);

        for (int i = 0; i < 5; i++) {
            ignite(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info(evt.shortDisplay());

                    failLatch.countDown();

                    return true;
                }
            }, EVT_NODE_FAILED);
        }

        Ignite ignite1 = ignite(0);
        Ignite ignite2 = ignite(1);

        final CountDownLatch evtLatch = new CountDownLatch(1);

        ignite1.message().localListen(null, new MessagingListenActor<Object>() {
            @Override protected void receive(UUID nodeId, Object rcvMsg) throws Throwable {
                respond(rcvMsg);
            }
        });

        ignite2.message().localListen(null, new MessagingListenActor<Object>() {
            @Override protected void receive(UUID nodeId, Object rcvMsg) throws Throwable {
                evtLatch.countDown();

                respond(rcvMsg);
            }
        });

        ignite1.message(ignite1.cluster().forRemotes()).send(null, "Message");

        evtLatch.await(); // Wait when connection is established.

        log.info("Fail node: " + ignite1.cluster().localNode());

        failNode(ignite1);

        assert failLatch.await(1500, MILLISECONDS);
    }

    /**
     * @param ignite Ignite.
     * @throws Exception In case of error.
     */
    private void failNode(Ignite ignite) throws Exception {
        DiscoverySpi disco = ignite.configuration().getDiscoverySpi();

        U.invoke(disco.getClass(), disco, "simulateNodeFailure");

        CommunicationSpi comm = ignite.configuration().getCommunicationSpi();

        U.invoke(comm.getClass(), comm, "simulateNodeFailure");
    }
}
