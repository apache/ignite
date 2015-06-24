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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Test for {@link ClusterGroup#forHost(String, String...)}.
 *
 * @see GridProjectionSelfTest
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterForHostsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        Collection<String> hostNames = null;

        if ("forHostTest".equals(gridName))
            hostNames = Arrays.asList("h_1", "h_2", "h_3");

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (hostNames != null) {
            TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

            cfg.setDiscoverySpi(new CustomHostsTcpDiscoverySpi(hostNames).setIpFinder(disco.getIpFinder()));
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testForHosts() throws Exception {
        Ignite ignite = startGrid("forHostTest");

        assertEquals(1, ignite.cluster().forHost("h_1").nodes().size());
        assertEquals(1, ignite.cluster().forHost("h_1", "h_3").nodes().size());
        assertEquals(1, ignite.cluster().forHost("unknown_host", "h_2").nodes().size());
        assertEquals(1, ignite.cluster().forHost("h_1", "h_3", "unknown_host", "h_2").nodes().size());

        assertEquals(0, ignite.cluster().forHost("unknown_host").nodes().size());

        boolean gotNpe = false;

        try {
            assertEquals(0, ignite.cluster().forHost(null, null, null).nodes().size());
        }
        catch (NullPointerException e) {
            gotNpe = true;
        }

        assertTrue(gotNpe);
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
