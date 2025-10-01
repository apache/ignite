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
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.function.ThrowableSupplier;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

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

    /** */
    @Test
    public void testTwoConflictVersions() {
        testConflictVersions("2.18.0", "2.16.0", false);
        testConflictVersions("2.19.0", "2.17.6", false);
        testConflictVersions("2.20.0", "2.22.2", false);
        testConflictVersions("2.21.0", "2.23.1", false);
    }

    /** */
    @Test
    public void testThreeConflictVersions() {
        testConflictVersions("2.18.0", "2.18.2", "2.16.0", false);
        testConflictVersions("2.18.0", "2.18.3", "2.20.0", false);

        testConflictVersions("2.18.0", "2.19.2", "2.17.0", false);
        testConflictVersions("2.18.0", "2.17.3", "2.19.0", false);

        testConflictVersions("2.18.0", "2.19.2", "2.20.0", false);
        testConflictVersions("2.18.0", "2.17.3", "2.16.0", false);
    }

    /** */
    @Test
    public void testTwoConflictVersionsWithClient() {
        testConflictVersions("2.18.0", "2.16.0", true);
        testConflictVersions("2.19.0", "2.17.6", true);
        testConflictVersions("2.20.0", "2.22.2", true);
        testConflictVersions("2.21.0", "2.23.1", true);
    }

    /** */
    @Test
    public void testThreeConflictVersionsWithClients() {
        testConflictVersions("2.18.0", "2.18.2", "2.16.0", true);
        testConflictVersions("2.18.0", "2.18.3", "2.20.0", true);

        testConflictVersions("2.18.0", "2.19.2", "2.17.0", true);
        testConflictVersions("2.18.0", "2.17.3", "2.19.0", true);

        testConflictVersions("2.18.0", "2.19.2", "2.20.0", true);
        testConflictVersions("2.18.0", "2.17.3", "2.16.0", true);
    }

    /**
     */
    @Test
    public void tesTwoCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.17.0", false);
        testCompatibleVersions("2.19.0", "2.20.6", false);
        testCompatibleVersions("2.20.0", "2.20.2", false);
        testCompatibleVersions("2.21.0", "2.21.1", false);
    }

    /**
     */
    @Test
    public void tesThreeCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.2", "2.17.0", false);
        testCompatibleVersions("2.18.0", "2.18.3", "2.19.0", false);

        testCompatibleVersions("2.18.0", "2.19.2", "2.18.1", false);
        testCompatibleVersions("2.18.0", "2.17.3", "2.18.2", false);

        testCompatibleVersions("2.18.0", "2.19.2", "2.19.6", false);
        testCompatibleVersions("2.18.0", "2.17.3", "2.17.1", false);

        testCompatibleVersions("2.18.1", "2.18.2", "2.18.3", false);
    }

    /**
     */
    @Test
    public void tesTwoCompatibleVersionsWithClient() throws Exception {
        testCompatibleVersions("2.18.0", "2.17.0", true);
        testCompatibleVersions("2.19.0", "2.20.6", true);
        testCompatibleVersions("2.20.0", "2.20.2", true);
        testCompatibleVersions("2.21.0", "2.21.1", true);
    }

    /**
     */
    @Test
    public void tesThreeCompatibleVersionsWithClients() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.2", "2.17.0", true);
        testCompatibleVersions("2.18.0", "2.18.3", "2.19.0", true);

        testCompatibleVersions("2.18.0", "2.19.2", "2.18.1", true);
        testCompatibleVersions("2.18.0", "2.17.3", "2.18.2", true);

        testCompatibleVersions("2.18.0", "2.19.2", "2.19.6", true);
        testCompatibleVersions("2.18.0", "2.17.3", "2.17.1", true);

        testCompatibleVersions("2.18.1", "2.18.2", "2.18.3", true);
    }

    /** */
    @Test
    public void testRollingUpgrade0() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false);
        IgniteEx ign1 = startGrid(1, "2.18.0", false);
        IgniteEx ign2 = startGrid(2, "2.18.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        ign0.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(3, "2.19.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        ign1.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(4, "2.19.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        ign2.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(5, "2.19.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));
    }

    /** */
    @Test
    public void testRollingUpgrade1() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false);
        IgniteEx ign1 = startGrid(1, "2.19.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        ign0.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 1, getTestTimeout()));

        ign0 = startGrid(2, "2.20.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        ign1.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 1, getTestTimeout()));

        startGrid(3, "2.21.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));
    }

    /** */
    private void testConflictVersions(String acceptedVer, String rejVer, boolean withClient) {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer, false);

            startGrid(1, rejVer, withClient);

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();
    }

    /** */
    private void testConflictVersions(String acceptedVer1, String acceptedVer2, String rejVer, boolean withClients) {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer1, false);

            startGrid(1, acceptedVer2, withClients);

            startGrid(2, rejVer, withClients);

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();
    }

    /** */
    private void assertRemoteRejected(ThrowableSupplier<IgniteEx, Exception> gridStart) {
        Throwable e = assertThrows(log, gridStart::get, IgniteCheckedException.class, null);

        StringWriter errors = new StringWriter();

        e.printStackTrace(new PrintWriter(errors));

        String stackTrace = errors.toString();

        assert stackTrace.contains("Remote node rejected due to incompatible version for cluster join");
    }

    /** */
    private void testCompatibleVersions(String acceptedVer1, String acceptedVer2, boolean withClient) throws Exception {
        startGrid(0, acceptedVer1, false);
        startGrid(1, acceptedVer2, withClient);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        stopAllGrids();
    }

    /** */
    private void testCompatibleVersions(
        String acceptedVer1,
        String acceptedVer2,
        String acceptedVer3,
        boolean withClients
    ) throws Exception {
        startGrid(0, acceptedVer1, false);
        startGrid(1, acceptedVer2, withClients);
        startGrid(2, acceptedVer3, withClients);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        stopAllGrids();
    }

    /** */
    private IgniteEx startGrid(int idx, String ver, boolean isClient) throws Exception {
        nodeVer = ver;

        return isClient ? startClientGrid(idx) : startGrid(idx);
    }
}
