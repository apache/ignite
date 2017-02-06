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

package org.apache.ignite.internal;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link ClusterGroup#forHost(String, String...)}.
 *
 * @see ClusterGroupSelfTest
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterGroupHostsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        Collection<String> hostNames = Arrays.asList("h_1", "h_2", "h_3");

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        cfg.setDiscoverySpi(new CustomHostsTcpDiscoverySpi(hostNames).setIpFinder(disco.getIpFinder()));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testForHosts() throws Exception {
        Ignite ignite = grid();

        assertEquals(1, ignite.cluster().forHost("h_1").nodes().size());
        assertEquals(1, ignite.cluster().forHost("h_1", "h_3").nodes().size());
        assertEquals(1, ignite.cluster().forHost("unknown_host", "h_2").nodes().size());
        assertEquals(1, ignite.cluster().forHost("h_1", "h_3", "unknown_host", "h_2").nodes().size());

        assertEquals(0, ignite.cluster().forHost("unknown_host").nodes().size());

        boolean gotNpe = false;

        try {
            assertEquals(0, ignite.cluster().forHost(null, null, null).nodes().size());
        }
        catch (NullPointerException ignored) {
            gotNpe = true;
        }
        finally {
            assertTrue(gotNpe);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHostNames() throws Exception {
        Ignite ignite = grid();

        Collection<String> locNodeHosts = ignite.cluster().localNode().hostNames();
        Collection<String> clusterHosts = ignite.cluster().hostNames();

        assertTrue(F.eqNotOrdered(locNodeHosts, clusterHosts));

        boolean gotNpe = false;

        try {
            clusterHosts.add("valueShouldNotToBeAdded");
        }
        catch (UnsupportedOperationException ignored) {
            gotNpe = true;
        }
        finally {
            assertTrue(gotNpe);
        }
    }

    /**
     * Tcp discovery spi that allow to customise hostNames of created local node.
     */
    private static class CustomHostsTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Hosts. */
        private final Collection<String> hosts;

        /**
         * @param hosts Host names which will be retuned by {@link ClusterNode#hostNames()} of created local node.
         */
        CustomHostsTcpDiscoverySpi(Collection<String> hosts) {
            this.hosts = hosts;
        }

        /**
         * @param srvPort Server port.
         */
        @Override protected void initLocalNode(int srvPort, boolean addExtAddrAttr) {
            super.initLocalNode(srvPort, addExtAddrAttr);

            try {
                Field hostNamesField = locNode.getClass().getDeclaredField("hostNames");

                hostNamesField.setAccessible(true);

                hostNamesField.set(locNode, hosts);
            }
            catch (IllegalAccessException | NoSuchFieldException e) {
                U.error(log, "Looks like implementation of " + locNode.getClass()
                    + " class was changed. Need to update test.", e);
            }
        }
    }
}
