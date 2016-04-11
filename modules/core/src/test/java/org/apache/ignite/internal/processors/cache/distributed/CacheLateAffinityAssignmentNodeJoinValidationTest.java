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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheLateAffinityAssignmentNodeJoinValidationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean lateAff;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLateAffinityAssignment(lateAff);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinValidation1() throws Exception {
        checkNodeJoinValidation(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinValidation2() throws Exception {
        checkNodeJoinValidation(true);
    }

    /**
     * @param firstEnabled Flag value for first started node.
     * @throws Exception If failed.
     */
    public void checkNodeJoinValidation(boolean firstEnabled) throws Exception {
        lateAff = firstEnabled;

        Ignite ignite = startGrid(0);

        assertFalse(ignite.configuration().isClientMode());

        lateAff = !firstEnabled;

        try {
            startGrid(1);

            fail();
        }
        catch (Exception e) {
            checkError(e);
        }

        client = true;

        try {
            startGrid(1);

            fail();
        }
        catch (Exception e) {
            checkError(e);
        }

        assertEquals(1, ignite.cluster().nodes().size());

        lateAff = firstEnabled;

        client = false;

        startGrid(1);

        client = true;

        Ignite client = startGrid(2);

        assertTrue(client.configuration().isClientMode());
    }

    /**
     * @param e Error.
     */
    private void checkError(Exception e) {
        IgniteSpiException err = X.cause(e, IgniteSpiException.class);

        assertNotNull(err);
        assertTrue(err.getMessage().contains("Local node's cache affinity assignment mode differs " +
            "from the same property on remote node"));
    }
}
