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

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Tests build version setting into discovery maps.
 */
public class GridTopologyBuildVersionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Counter. */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** Test compatible versions. */
    private static final Collection<String> COMPATIBLE_VERS =
        F.asList("1.0.0-ent", "2.0.0-ent", "3.0.0-ent", "4.0.0-ent");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        final int idx = cnt.incrementAndGet();

        // Override node attributes in discovery spi.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                attrs.put(GridNodeAttributes.ATTR_BUILD_VER, idx + ".0.0" + "-ent");

                if (idx < 3)
                    attrs.remove(GridNodeAttributes.ATTR_BUILD_DATE);
                else
                    attrs.put(GridNodeAttributes.ATTR_BUILD_DATE, "1385099743");

                attrs.put(GridNodeAttributes.ATTR_COMPATIBLE_VERS, COMPATIBLE_VERS);
            }
        };

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersioning() throws Exception {
        startGrids(4);

        try {
            for (int i = 3; i >= 0; i--) {
                IgniteKernal g = (IgniteKernal)grid(i);

                NavigableMap<IgniteProductVersion, Collection<ClusterNode>> verMap = g.context().discovery()
                    .topologyVersionMap();

                assertEquals(4, verMap.size());

                // Now check the map itself.
                assertEquals(4, verMap.get(IgniteProductVersion.fromString("1.0.0")).size());
                assertEquals(3, verMap.get(IgniteProductVersion.fromString("2.0.0")).size());
                assertEquals(2, verMap.get(IgniteProductVersion.fromString("3.0.0-ent-1385099743")).size());
                assertEquals(1, verMap.get(IgniteProductVersion.fromString("4.0.0-ent-1385099743")).size());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
