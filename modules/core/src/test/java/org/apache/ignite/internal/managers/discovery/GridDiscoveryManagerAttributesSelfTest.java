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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Tests for node attributes consistency checks.
 */
public abstract class GridDiscoveryManagerAttributesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String PREFER_IPV4 = "java.net.preferIPv4Stack";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static DeploymentMode mode;

    /** */
    private static boolean p2pEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(1)))
            cfg.setClientMode(true);

        cfg.setIncludeProperties(PREFER_IPV4);
        cfg.setDeploymentMode(mode);
        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoverySpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mode = SHARED;

        p2pEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreferIpV4StackTrue() throws Exception {
        testPreferIpV4Stack(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreferIpV4StackFalse() throws Exception {
        testPreferIpV4Stack(false);
    }

    /**
     * This test should output warning to log on 3rd grid start:
     * <pre>
     * [10:47:05,534][WARN ][Thread-68][GridDiscoveryManager] Local node's value of 'java.net.preferIPv4Stack'
     * system property differs from remote node's (all nodes in topology should have identical value)
     * [locPreferIpV4=false, rmtPreferIpV4=true, locId8=b1cad004, rmtId8=16193477]
     * </pre>
     *
     * @throws Exception If failed.
     */
    public void testPreferIpV4StackDifferentValues() throws Exception {
        System.setProperty(PREFER_IPV4, "true");

        for (int i = 0; i < 2; i++) {
            Ignite g = startGrid(i);

            assert "true".equals(g.cluster().localNode().attribute(PREFER_IPV4));
        }

        System.setProperty(PREFER_IPV4, "false");

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUseDefaultSuid() throws Exception {
        try {
            doTestUseDefaultSuid(Boolean.TRUE.toString(), Boolean.FALSE.toString(), true);
            doTestUseDefaultSuid(Boolean.FALSE.toString(), Boolean.TRUE.toString(), true);

            doTestUseDefaultSuid(Boolean.TRUE.toString(), Boolean.TRUE.toString(), false);
            doTestUseDefaultSuid(Boolean.FALSE.toString(), Boolean.FALSE.toString(), false);
        }
        finally {
            System.setProperty(IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID,
                String.valueOf(OptimizedMarshaller.USE_DFLT_SUID));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestUseDefaultSuid(String first, String second, boolean fail) throws Exception {
        try {
            System.setProperty(IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID, first);

            startGrid(0);

            System.setProperty(IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID, second);

            try {
                startGrid(1);

                if (fail)
                    fail("Node should not join");
            }
            catch (Exception e) {
                if (!fail)
                    fail("Node should join");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentDeploymentModes() throws Exception {
        startGrid(0);

        mode = CONTINUOUS;

        try {
            startGrid(1);

            fail();
        }
        catch (IgniteCheckedException e) {
            if (!e.getCause().getMessage().startsWith("Remote node has deployment mode different from"))
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentPeerClassLoadingEnabledFlag() throws Exception {
        startGrid(0);

        p2pEnabled = true;

        try {
            startGrid(1);

            fail();
        }
        catch (IgniteCheckedException e) {
            if (!e.getCause().getMessage().startsWith("Remote node has peer class loading enabled flag different from"))
                throw e;
        }
    }

    /**
     * @param preferIpV4 {@code java.net.preferIPv4Stack} system property value.
     * @throws Exception If failed.
     */
    private void testPreferIpV4Stack(boolean preferIpV4) throws Exception {
        String val = String.valueOf(preferIpV4);

        System.setProperty(PREFER_IPV4, val);

        for (int i = 0; i < 2; i++) {
            Ignite g = startGrid(i);

            assert val.equals(g.cluster().localNode().attribute(PREFER_IPV4));
        }
    }

    /**
     *
     */
    public static class RegularDiscovery extends GridDiscoveryManagerAttributesSelfTest {
        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
            IgniteConfiguration cfg = super.getConfiguration(gridName);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

            return cfg;
        }
    }

    /**
     *
     */
    public static class ClientDiscovery extends GridDiscoveryManagerAttributesSelfTest {
        // No-op.
    }
}
