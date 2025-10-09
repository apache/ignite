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

import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.function.ThrowableSupplier;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.nodevalidation.OsDiscoveryNodeValidationProcessor.ROLL_UP_VERSION_CHECK;
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
        testConflictVersions("2.21.0", "2.23.1", false);
        testConflictVersions("2.20.1", "2.20.2", false);
    }

    /** */
    @Test
    public void testThreeConflictVersions() {
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.2", "2.16.0", false, "2.18.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.3", "2.20.0", false, "2.18.3");

        testConflictVersionsWithRollingUpgrade("2.18.0", "2.19.2", "2.17.0", false, "2.19.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.17.3", "2.19.0", false, "2.17.3");

        testConflictVersionsWithRollingUpgrade("2.18.0", "2.19.2", "2.20.0", false, "2.19.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.17.3", "2.16.0", false, "2.17.3");
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
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.2", "2.16.0", true, "2.18.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.3", "2.20.0", true, "2.18.3");

        testConflictVersionsWithRollingUpgrade("2.18.0", "2.19.2", "2.17.0", true, "2.19.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.17.3", "2.19.0", true, "2.17.3");

        testConflictVersionsWithRollingUpgrade("2.18.0", "2.19.2", "2.20.0", true, "2.19.2");
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.17.3", "2.16.0", true, "2.17.3");
    }

    /** */
    @Test
    public void testTwoCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", false, null);
        testCompatibleVersions("2.19.0", "2.19.0", false, null);

        testCompatibleVersions("2.18.0", "2.18.1", false, "2.18.1");
        testCompatibleVersions("2.19.0", "2.19.6", false, "2.19.6");
    }

    /** */
    @Test
    public void testThreeCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", "2.18.0", false, null);
        testCompatibleVersions("2.18.2", "2.18.2", "2.18.2", false, null);

        testCompatibleVersions("2.18.0", "2.18.1", "2.18.1", false, "2.18.1");
        testCompatibleVersions("2.18.0", "2.18.2", "2.18.2", false, "2.18.2");
    }

    /** */
    @Test
    public void testTwoCompatibleVersionsWithClient() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", true, null);
        testCompatibleVersions("2.19.0", "2.19.0", true, null);
        testCompatibleVersions("2.20.2", "2.20.2", true, null);

        testCompatibleVersions("2.18.0", "2.18.1", true, "2.18.1");
        testCompatibleVersions("2.19.0", "2.19.6", true, "2.19.6");
    }

    /** */
    @Test
    public void testThreeCompatibleVersionsWithClients() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", "2.18.0", true, null);
        testCompatibleVersions("2.18.1", "2.18.1", "2.18.1", true, null);

        testCompatibleVersions("2.18.0", "2.18.1", "2.18.1", true, "2.18.1");
        testCompatibleVersions("2.18.0", "2.18.2", "2.18.2", true, "2.18.2");
    }

    /** */
    @Test
    public void testForwardRollingUpgrade() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false);
        IgniteEx ign1 = startGrid(1, "2.18.0", false);
        IgniteEx ign2 = startGrid(2, "2.18.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        assertRemoteRejected(() -> startGrid(3, "2.18.1", false));

        allowRollingUpgradeVersionCheck(ign0, "2.18.1");

        assertRemoteRejected(() -> startGrid(3, "2.18.0", false));

        ign2.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(2, "2.18.1", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        ign1.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(1, "2.18.1", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        ign0.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        startGrid(0, "2.18.1", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));
    }

    /** */
    @Test
    public void testCoordinatorChange() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false);

        allowRollingUpgradeVersionCheck(ign0, "2.19.0");

        IgniteEx ign1 = startGrid(1, "2.19.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        ign0.close();

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 1, getTestTimeout()));

        assertRemoteRejected(() -> startGrid(0, "2.18.0", false));

        allowRollingUpgradeVersionCheck(ign1, "2.20.0");

        startGrid(0, "2.20.0", false);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        assertRemoteRejected(() -> startGrid(2, "2.21.0", false));

        assertTrue(Ignition.allGrids().size() == 2);
    }

    /** Tests that starting a node with rejected version fails with remote rejection. */
    private void testConflictVersions(String acceptedVer, String rejVer, boolean isClient) {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer, false);

            startGrid(1, rejVer, isClient);

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();
    }

    /** Checks that the third grid is not compatible when rolling upgrade version is set. */
    private void testConflictVersionsWithRollingUpgrade(String acceptedVer1, String acceptedVer2, String rejVer,
        boolean isClient, String rollUpVer) {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer1, false);

            allowRollingUpgradeVersionCheck(ign, rollUpVer);

            startGrid(1, acceptedVer2, isClient);

            startGrid(2, rejVer, isClient);

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();
    }

    /** Checks that remote node rejected due to incompatible version. */
    private void assertRemoteRejected(ThrowableSupplier<IgniteEx, Exception> gridStart) {
        Throwable e = assertThrows(log, gridStart::get, IgniteCheckedException.class, null);

        assertTrue(X.hasCause(e, "Remote node rejected due to incompatible version for cluster join", IgniteSpiException.class));
    }

    /** Tests two compatible grids. */
    private void testCompatibleVersions(String acceptedVer1,
        String acceptedVer2,
        boolean isClient,
        String rollUpVerCheck) throws Exception {
        IgniteEx grid = startGrid(0, acceptedVer1, false);

        allowRollingUpgradeVersionCheck(grid, rollUpVerCheck);

        startGrid(1, acceptedVer2, isClient);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 2, getTestTimeout()));

        stopAllGrids();
    }

    /** Tests three compatible grids. */
    private void testCompatibleVersions(
        String acceptedVer1,
        String acceptedVer2,
        String acceptedVer3,
        boolean isClient,
        String rollUpVerCheck
    ) throws Exception {
        IgniteEx grid = startGrid(0, acceptedVer1, false);

        allowRollingUpgradeVersionCheck(grid, rollUpVerCheck);

        startGrid(1, acceptedVer2, isClient);
        startGrid(2, acceptedVer3, isClient);

        assertTrue(waitForCondition(() -> Ignition.allGrids().size() == 3, getTestTimeout()));

        stopAllGrids();
    }

    /** Starts grid with required version. */
    private IgniteEx startGrid(int idx, String ver, boolean isClient) throws Exception {
        return startGrid(idx, ver, isClient, null);
    }

    /** Starts grid with required version and custom configuration. */
    private IgniteEx startGrid(int idx, String ver, boolean isClient, UnaryOperator<IgniteConfiguration> cfgOp) throws Exception {
        nodeVer = ver;

        return isClient ? startClientGrid(idx, cfgOp) : startGrid(idx, cfgOp);
    }

    /**
     * @param ver Version for rolling upgrade support.
     */
    private void allowRollingUpgradeVersionCheck(IgniteEx grid, String ver) throws IgniteCheckedException {
        if (ver == null) {
            grid.context().distributedMetastorage().remove(ROLL_UP_VERSION_CHECK);
            return;
        }

        grid.context().distributedMetastorage().write(ROLL_UP_VERSION_CHECK, IgniteProductVersion.fromString(ver));
    }
}
