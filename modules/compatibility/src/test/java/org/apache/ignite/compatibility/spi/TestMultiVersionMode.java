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

package org.apache.ignite.compatibility.spi;

import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/** */
public class TestMultiVersionMode extends IgniteCompatibilityAbstractTest {
    /** */
    public void testJoinMultiVersionTopologyLocalFirst() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            assertEquals(1, topologyVersion(ignite));

            startGrid("testMultiVersion", "2.1.0", new PostConfigurationClosure());

            assertEquals(2, topologyVersion(ignite));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testJoinMultiVersionTopologyRemoteFirst() throws Exception {
        try {
            startGrid(1, "2.1.0", new PostConfigurationClosure());

            IgniteEx ignite = startGrid(0);

            assertEquals(2, topologyVersion(ignite));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testJoinMultiVersionTopologyRemoteFirst2() throws Exception {
        try {
            startGrid(1, "2.1.0", new PostConfigurationClosure());

            IgniteEx ignite = startGrid(0);

            assertEquals(2, topologyVersion(ignite));

            startGrid(3, "2.1.0", new PostConfigurationClosure());

            assertEquals(3, topologyVersion(ignite));

            startGrid(4);

            assertEquals(4, topologyVersion(ignite));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testJoinMultiVersionTopologyLocalFirst2() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            assertEquals(1, topologyVersion(ignite));

            startGrid(1, "2.1.0", new PostConfigurationClosure());

            assertEquals(2, topologyVersion(ignite));

            startGrid(2, "2.1.0", new PostConfigurationClosure());

            assertEquals(3, topologyVersion(ignite));

            startGrid(3, "2.1.0", new PostConfigurationClosure());

            assertEquals(4, topologyVersion(ignite));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");
            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);
            cfg.setDiscoverySpi(disco);
        }
    }

    /**
     * @param ignite Ignite instance.
     * @return Topology version.
     */
    private static long topologyVersion(IgniteEx ignite) {
        return ignite.context().discovery().topologyVersion();
    }
}
