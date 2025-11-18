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

package org.apache.ignite.spi.discovery.datacenter;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
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

        for (int i = 0; i < 10; i += 2) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_0 : DC_ID_1);

            startGrid(i);

            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, order ? DC_ID_1 : DC_ID_0);

            startGrid(i + 1);
        }

        waitForTopology(10);

        Collection<ClusterNode> nodes = grid(0).cluster().forServers().nodes();

        int swithes = 0;
        String curDcId = null;

        for (ClusterNode node : nodes) {
            if (!node.dataCenterId().equals(curDcId)){
                swithes++;

                curDcId = node.dataCenterId();
            }
        }

        assertEquals(2, swithes);
    }
}
