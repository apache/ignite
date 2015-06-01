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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Test grids starting with non compatible release types.
 */
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private String nodeVer;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (clientMode)
            cfg.setClientMode(true);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs,
                IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                attrs.put(IgniteNodeAttributes.ATTR_BUILD_VER, nodeVer);
            }
        };

        discoSpi.setIpFinder(IP_FINDER).setForceServerMode(true);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clientMode = false;

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOsEditionDoesNotSupportRollingUpdates() throws Exception {
        nodeVer = "1.0.0";

        startGrid(0);

        try {
            nodeVer = "1.0.1";

            startGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Local node and remote node have different version numbers"))
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOsEditionDoesNotSupportRollingUpdatesClientMode() throws Exception {
        nodeVer = "1.0.0";

        startGrid(0);

        try {
            nodeVer = "1.0.1";
            clientMode = true;

            startGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Local node and remote node have different version numbers"))
                throw e;
        }
    }
}
