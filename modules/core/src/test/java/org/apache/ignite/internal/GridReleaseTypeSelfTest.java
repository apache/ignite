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
import java.util.concurrent.atomic.*;

/**
 * Test grids starting with non compatible release types.
 */
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Counter. */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** */
    private String firstNodeVer;

    /** */
    private String secondNodeVer;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        final int idx = cnt.getAndIncrement();

        // Override node attributes in discovery spi.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                if (idx % 2 == 0)
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, firstNodeVer);
                else
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, secondNodeVer);
            }
        };

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinTopologyWithDifferentReleaseType() throws Exception {
        firstNodeVer = "1.0.0-ent";
        secondNodeVer = "1.0.0-os";

        try {
            startGrids(2);
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            assertTrue(
                "Caught exception does not contain specified string.",
                stackTrace.contains("Topology cannot contain nodes of both enterprise and open source")
            );

            return;
        }
        finally {
            stopAllGrids();
        }

        fail("Exception has not been thrown.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testOsEditionDoesNotSupportRollingUpdates() throws Exception {
        firstNodeVer = "1.0.0-os";
        secondNodeVer = "1.0.1-os";

        try {
            startGrids(2);
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            assertTrue(
                "Caught exception does not contain specified string.",
                stackTrace.contains("Local node and remote node have different version numbers")
            );

            return;
        }
        finally {
            stopAllGrids();
        }

        fail("Exception has not been thrown.");
    }
}
