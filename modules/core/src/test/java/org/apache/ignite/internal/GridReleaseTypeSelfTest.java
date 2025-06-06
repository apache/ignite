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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test Rolling Upgrade release types.
 */
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** */
    private String nodeVer;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs,
                IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                attrs.put(IgniteNodeAttributes.ATTR_BUILD_VER, nodeVer);
            }
        };

        discoSpi.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgradeConflictVersions() throws Exception {
        nodeVer = "2.20.0";

        startGrid(0);

        try {
            nodeVer = "2.17.1";

            startGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Incompatible version for cluster join"))
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgradeConflictVersionsWithClient() throws Exception {
        nodeVer = "2.20.0";

        startGrid(0);

        try {
            nodeVer = "2.17.1";

            startClientGrid(1);

            fail("Exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            if (!stackTrace.contains("Incompatible version for cluster join"))
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgradeCompatibleVersions() throws Exception {
        nodeVer = "2.20.0";

        startGrid(0);

        nodeVer = "2.18.1";

        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgradeCompatibleVersionsWithClient() throws Exception {
        nodeVer = "2.20.0";

        startGrid(0);

        nodeVer = "2.18.1";

        startClientGrid(1);
    }
}
