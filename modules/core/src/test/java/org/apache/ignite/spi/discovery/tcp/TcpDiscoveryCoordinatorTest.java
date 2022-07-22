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

import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for TcpDiscoverySpi coordinator tracking
 */
public class TcpDiscoveryCoordinatorTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super
                .getConfiguration(igniteInstanceName)
                .setDaemon("daemon".equals(igniteInstanceName));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testGetCoordinatorWithOldesDaemonNode() throws Exception {
        //Start cluster with server1 as coordinator.
        startGrid("server1");
        startGrid("daemon");
        IgniteEx srv2 = startGrid("server2");

        //Change topology to make server2 new coordinator
        stopGrid("server1");

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)srv2.configuration().getDiscoverySpi();
        UUID spiCrd = discoverySpi.getCoordinator();

        assertEquals(srv2.localNode().id(), spiCrd);
    }
}
