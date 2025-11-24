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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.function.ThrowableSupplier;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeTrue;

/**
 * Test Rolling Upgrade release types.
 */
@RunWith(Parameterized.class)
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** */
    private String nodeVer;

    /**
     * Indicates whether the tested node is started as a client.
     * This flag is used to run all test cases for both client and server node configurations.
     */
    @Parameterized.Parameter
    public boolean client;

    /** Persistence. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "client={0}, persistence={1}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(List.of(false, true), List.of(false, true));
    }

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

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testTwoConflictVersions() {
        testConflictVersions("2.18.0", "2.16.0", client);
        testConflictVersions("2.21.0", "2.23.1", client);
        testConflictVersions("2.20.1", "2.20.2", client);
    }

    /** */
    @Test
    public void testThreeConflictVersions() throws Exception {
        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.1", "2.18.2", client, "2.18.1");

        testConflictVersionsWithRollingUpgrade("2.18.0", "2.18.1", "2.17.2", client, "2.18.1");

        testConflictVersionsWithRollingUpgrade("2.18.1", "2.19.0", "2.19.1", client, "2.19.0");

        testConflictVersionsWithRollingUpgrade("2.18.1", "2.18.2", "2.18.0", client, "2.18.2");
    }

    /** */
    @Test
    public void testTwoCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", client, null);
        testCompatibleVersions("2.19.2", "2.19.2", client, null);

        testCompatibleVersions("2.18.0", "2.18.1", client, "2.18.1");
        testCompatibleVersions("2.18.2", "2.19.0", client, "2.19.0");
    }

    /** */
    @Test
    public void testThreeCompatibleVersions() throws Exception {
        testCompatibleVersions("2.18.0", "2.18.0", "2.18.0", client, null);
        testCompatibleVersions("2.18.2", "2.18.2", "2.18.2", client, null);

        testCompatibleVersions("2.18.0", "2.18.1", "2.18.1", client, "2.18.1");
        testCompatibleVersions("2.18.1", "2.19.0", "2.18.1", client, "2.19.0");
    }

    /** */
    @Test
    public void testForwardRollingUpgrade() throws Exception {
        doTestRollingUpgrade("2.18.0", "2.18.1", false);
    }

    /** */
    @Test
    public void testForceRollingUpgrade() throws Exception {
        doTestRollingUpgrade("2.18.0", "2.19.1", true);
    }

    /** Performs full rolling upgrade scenario. */
    private void doTestRollingUpgrade(String curVer, String targetVer, boolean force) throws Exception {
        IgniteEx ign0 = startGrid(0, curVer, false);
        IgniteEx ign1 = startGrid(1, curVer, client);
        IgniteEx ign2 = startGrid(2, curVer, client);

        assertClusterSize(3);

        assertRemoteRejected(() -> startGrid(3, targetVer, client));

        configureRollingUpgradeVersion(ign0, targetVer, force);

        for (int i = 0; i < 3; i++) {
            int finalI = i;
            assertTrue(waitForCondition(() -> grid(finalI).context().rollingUpgrade().enabled(), getTestTimeout()));
        }

        ign2.close();

        assertClusterSize(2);

        startGrid(2, targetVer, client);

        assertClusterSize(3);

        ign1.close();

        assertClusterSize(2);

        startGrid(1, targetVer, client);

        assertClusterSize(3);

        ign0.close();

        assertClusterSize(2);

        startGrid(0, targetVer, false);

        assertClusterSize(3);

        if (client)
            grid(0).context().rollingUpgrade().disable();
        else
            grid(2).context().rollingUpgrade().disable();

        for (int i = 0; i < 3; i++) {
            if (!grid(i).localNode().isClient())
                assertFalse(grid(i).context().rollingUpgrade().enabled());
        }

        assertRemoteRejected(() -> startGrid(3, curVer, client));
    }

    /** */
    @Test
    public void testJoiningNodeFailed() throws Exception {
        int joinTimeout = 5_000;

        IgniteEx ign0 = startGrid(0, "2.18.0", false,
            cfg -> {
                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(joinTimeout);
                return cfg;
            });

        configureRollingUpgradeVersion(ign0, "2.18.1");

        RunnableX runnableX = () -> startGrid(1, "2.18.1", false,
            cfg -> {
                TcpDiscoverySpi oldSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

                TcpDiscoverySpi newSpi = new TcpDiscoverySpi() {
                    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                        super.setNodeAttributes(attrs, ver);

                        attrs.put(IgniteNodeAttributes.ATTR_BUILD_VER, nodeVer);
                        attrs.put(IgniteNodeAttributes.ATTR_MARSHALLER, "null");
                    }
                };

                newSpi.setIpFinder(oldSpi.getIpFinder());

                return cfg.setDiscoverySpi(newSpi);
            });

        Throwable e = assertThrows(log, runnableX, IgniteException.class, null);

        assertTrue(X.hasCause(e, "Local node's marshaller differs from remote node's marshaller", IgniteSpiException.class));

        assertDisablingFails(ign0, "Can't disable rolling upgrade with different versions in cluster");

        doSleep(joinTimeout * 3);

        ign0.context().rollingUpgrade().disable();

        assertFalse(ign0.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testJoiningNodeLeft() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false, cfg -> {
                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(0);
                return cfg;
            });

        configureRollingUpgradeVersion(ign0, "2.18.1");

        try (IgniteEx ignore = startGrid(1, "2.18.1", false)) {
            assertClusterSize(2);
        }

        assertClusterSize(1);

        ign0.context().rollingUpgrade().disable();
    }

    /** */
    @Test
    public void testCoordinatorChange() throws Exception {
        IgniteEx ign0 = startGrid(0, "2.18.0", false);
        IgniteEx ign1 = startGrid(1, "2.18.0", false);

        configureRollingUpgradeVersion(ign0, "2.19.0");

        startGrid(2, "2.19.0", false);

        assertClusterSize(3);

        ign0.close();
        ign1.close();

        assertClusterSize(1);

        startGrid(0, "2.18.0", client);
        startGrid(1, "2.19.0", client);

        assertClusterSize(3);

        assertRemoteRejected(() -> startGrid(4, "2.20.0", client));

        assertClusterSize(3);
    }

    /** */
    @Test
    public void testNodeRestart() throws Exception {
        assumeTrue("Distributed metastorage is only preserved across restarts when persistence is enabled", persistence);

        for (int i = 0; i < 3; i++)
            startGrid(i, "2.18.0", false);

        assertClusterSize(3);

        configureRollingUpgradeVersion(grid(0), "2.18.1");

        for (int i = 0; i < 3; i++)
            grid(i).close();

        assertClusterSize(0);

        for (int i = 0; i < 3; i++)
            startGrid(i, "2.18.0", false);

        assertClusterSize(3);

        for (int i = 0; i < 3; i++) {
            assertTrue(grid(i).context().rollingUpgrade().enabled());

            IgnitePair<IgniteProductVersion> stored = grid(i).context().rollingUpgrade().versions();

            assertEquals(F.pair(IgniteProductVersion.fromString("2.18.0"), IgniteProductVersion.fromString("2.18.1")), stored);
        }
    }

    /** */
    @Test
    public void testRollingUpgradeProcessorVersionCheck() throws Exception {
        IgniteEx grid0 = startGrid(0, "2.18.0", false);
        startGrid(1, "2.18.0", client);

        assertClusterSize(2);

        assertEnablingFails(grid0, "3.0.0", "Major versions are different");
        assertEnablingFails(grid0, "2.19.2", "Minor version can only be incremented by 1");
        assertEnablingFails(grid0, "2.18.2", "Patch version can only be incremented by 1");

        IgnitePair<IgniteProductVersion> newPair = F.pair(IgniteProductVersion.fromString("2.18.0"),
            IgniteProductVersion.fromString("2.19.0"));

        grid0.context().rollingUpgrade().enable(newPair.get2(), false);

        assertEnablingFails(grid0, "2.18.1", "Rolling upgrade is already enabled with a different current and target version");

        for (int i = 0; i < 2; i++) {
            assertTrue(waitForCondition(grid(i).context().rollingUpgrade()::enabled, getTestTimeout()));

            assertEquals(newPair, grid(i).context().rollingUpgrade().versions());
        }
    }

    /**
     * Checks that enabling rolling upgrade fails with expected error message.
     *
     * @param ex Ex.
     * @param ver New version.
     * @param errMsg Expected error message.
     */
    private void assertEnablingFails(IgniteEx ex, String ver, String errMsg) {
        Throwable e = assertThrows(log,
            () -> ex.context().rollingUpgrade().enable(IgniteProductVersion.fromString(ver), false),
            IgniteException.class,
            null);

        assertTrue(e.getMessage().contains(errMsg));
    }

    /**
     * Checks that disabling rolling upgrade fails with expected error message.
     *
     * @param ex Ex.
     * @param errMsg Expected error message.
     */
    private void assertDisablingFails(IgniteEx ex, String errMsg) {
        Throwable e = assertThrows(log,
            () -> ex.context().rollingUpgrade().disable(),
            IgniteException.class,
            null);

        assertTrue(e.getMessage().contains(errMsg));
    }

    /** Tests that starting a node with rejected version fails with remote rejection. */
    private void testConflictVersions(String acceptedVer, String rejVer, boolean client) {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer, false);

            startGrid(1, rejVer, client, cfg -> {
                TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

                // Decrease network timeout to reduce waiting time for node failure
                // after it has been rejected by the coordinator due to version conflict.
                spi.setNetworkTimeout(1_000);

                return cfg;
            });

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();
    }

    /** Checks that the third grid is not compatible when rolling upgrade version is set. */
    private void testConflictVersionsWithRollingUpgrade(String acceptedVer1, String acceptedVer2, String rejVer,
        boolean client, String rollUpVer) throws Exception {
        ThrowableSupplier<IgniteEx, Exception> sup = () -> {
            IgniteEx ign = startGrid(0, acceptedVer1, false);

            configureRollingUpgradeVersion(ign, rollUpVer);

            startGrid(1, acceptedVer2, client);

            startGrid(2, rejVer, client);

            return ign;
        };

        assertRemoteRejected(sup);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Checks that remote node rejected due to incompatible version. */
    private void assertRemoteRejected(ThrowableSupplier<IgniteEx, Exception> gridStart) {
        Throwable e = assertThrows(log, gridStart::get, IgniteCheckedException.class, null);

        assertTrue(X.hasCause(e, "Remote node rejected due to incompatible version for cluster join", IgniteSpiException.class));
    }

    /** Tests two compatible grids. */
    private void testCompatibleVersions(String acceptedVer1,
        String acceptedVer2,
        boolean client,
        String rollUpVerCheck) throws Exception {
        IgniteEx grid = startGrid(0, acceptedVer1, false);

        if (rollUpVerCheck != null)
            configureRollingUpgradeVersion(grid, rollUpVerCheck);

        startGrid(1, acceptedVer2, client);

        assertClusterSize(2);

        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();
    }

    /** Tests three compatible grids. */
    private void testCompatibleVersions(
        String acceptedVer1,
        String acceptedVer2,
        String acceptedVer3,
        boolean client,
        String rollUpVerCheck
    ) throws Exception {
        IgniteEx grid = startGrid(0, acceptedVer1, false);

        if (rollUpVerCheck != null)
            configureRollingUpgradeVersion(grid, rollUpVerCheck);

        startGrid(1, acceptedVer2, client);
        startGrid(2, acceptedVer3, client);

        assertClusterSize(3);

        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();
    }

    /** Starts grid with required version. */
    private IgniteEx startGrid(int idx, String ver, boolean isClient) throws Exception {
        return startGrid(idx, ver, isClient, null);
    }

    /** Starts grid with required version and custom configuration. */
    private IgniteEx startGrid(int idx, String ver, boolean isClient, UnaryOperator<IgniteConfiguration> cfgOp) throws Exception {
        nodeVer = ver;

        IgniteEx ign = isClient ? startClientGrid(idx, cfgOp) : startGrid(idx, cfgOp);

        if (persistence)
            ign.cluster().state(ClusterState.ACTIVE);

        return ign;
    }

    /** */
    private void configureRollingUpgradeVersion(IgniteEx grid, String ver) throws IgniteCheckedException {
        configureRollingUpgradeVersion(grid, ver, false);
    }

    /**
     * @param ver Version for rolling upgrade support.
     * @param force Force rolling upgrade.
     */
    private void configureRollingUpgradeVersion(IgniteEx grid, String ver, boolean force) throws IgniteCheckedException {
        if (ver == null) {
            grid.context().rollingUpgrade().disable();
            return;
        }

        IgniteProductVersion target = IgniteProductVersion.fromString(ver);

        grid.context().rollingUpgrade().enable(target, force);
    }

    /**
     * @param size Expected cluster size.
     */
    private void assertClusterSize(int size) throws IgniteInterruptedCheckedException {
        assertTrue("Expected cluster size: " + size + ", but was: " + Ignition.allGrids().size(),
            waitForCondition(() -> Ignition.allGrids().size() == size, getTestTimeout()));
    }
}
