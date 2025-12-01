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

package org.apache.ignite.spi.discovery.tcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MultiDataCenterRignTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** */
    @Test
    public void testRing() throws Exception {
        int cnt = 10;

        generateRandomDcCluster(cnt);

        assertEquals(cnt, grid(0).cluster().nodes().size());

        checkHops(2);

        stopGrid(cnt - 1);
        stopGrid(0);

        assertEquals(cnt - 2, grid(1).cluster().nodes().size());

        checkHops(2);
    }

    /** */
    @Test
    public void testMessageOrder() throws Exception {
        int cnt = 10;

        generateRandomDcCluster(cnt);

        Collection<Ignite> nodes = G.allGrids();

        CountDownLatch latch = new CountDownLatch(cnt);
        List<String> dcs = new ArrayList<>();

        for (Ignite node : nodes) {
            DiscoverySpi disco = node.configuration().getDiscoverySpi();

            ((TcpDiscoverySpi)disco).addSendMessageListener(new IgniteInClosure<>() {
                @Override public void apply(TcpDiscoveryAbstractMessage msg) {
                    if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                        dcs.add(Ignition.localIgnite().cluster().localNode().dataCenterId());

                        latch.countDown();
                    }
                }
            });
        }

        startGrid(cnt + 1);

        latch.await();

        String curDC = null;
        int hops = 0;

        for (String dcId : dcs) {
            if (!dcId.equals(curDC)) {
                hops++;
                curDC = dcId;
            }
        }

        assertEquals(2, hops);
    }

    /** */
    private void generateRandomDcCluster(int cnt) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean order = rnd.nextBoolean();

        for (int i = 0; i < cnt; i += 2) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_0 : DC_ID_1);

            startGrid(i);

            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_1 : DC_ID_0);

            startGrid(i + 1);
        }

        waitForTopology(cnt);
    }

    /** */
    private void checkHops(int expected) {
        Collection<Ignite> nodes = G.allGrids();

        int hops = 0;

        for (Ignite node : nodes) {
            DiscoverySpi disco = node.configuration().getDiscoverySpi();

            ServerImpl serverImpl = U.field(disco, "impl");

            String nextDcId = serverImpl.ring().nextNode().dataCenterId();
            String locDcId = node.cluster().localNode().dataCenterId();

            if (!locDcId.equals(nextDcId))
                hops++;
        }

        assertEquals(expected, hops);
    }

    /** */
    @Test
    public void testDcReversedChange() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        startGrid(1);

        awaitPartitionMapExchange();
    }
}
