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

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
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
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean order = rnd.nextBoolean();

        int cnt = 10;

        for (int i = 0; i < cnt; i += 2) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_0 : DC_ID_1);

            startGrid(i);

            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_1 : DC_ID_0);

            startGrid(i + 1);
        }

        waitForTopology(cnt);

        assertEquals(cnt, grid(rnd.nextInt(cnt)).cluster().nodes().size());

        checkSwitches(2);

        stopGrid(cnt - 1);
        stopGrid(0);

        assertEquals(cnt - 2, grid(rnd.nextInt(cnt)).cluster().nodes().size());

        checkSwitches(2);
    }

    /** */
    private void checkSwitches(int expected) {
        Collection<Ignite> nodes = G.allGrids();

        int swithes = 0;

        for (Ignite node : nodes) {
            DiscoverySpi disco = node.configuration().getDiscoverySpi();

            ServerImpl serverImpl = U.field(disco, "impl");

            String nextDcId = serverImpl.ring().nextNode().dataCenterId();
            String localDcId = node.cluster().localNode().dataCenterId();

            if (!localDcId.equals(nextDcId)) {
                swithes++;
            }
        }

        assertEquals(expected, swithes);
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
