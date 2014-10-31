/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.discovery;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.GridDeploymentMode.*;

/**
 * Tests for node attributes consistency checks.
 */
public class GridDiscoveryManagerAttributesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String PREFER_IPV4 = "java.net.preferIPv4Stack";

    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static GridDeploymentMode mode = SHARED;

    /** */
    private static boolean p2pEnabled;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

        disc.setIpFinder(IP_FINDER);

        cfg.setIncludeProperties(PREFER_IPV4);
        cfg.setDeploymentMode(mode);
        cfg.setPeerClassLoadingEnabled(p2pEnabled);
        cfg.setDiscoverySpi(disc);

        return cfg;
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
        try {
            System.setProperty(PREFER_IPV4, "true");

            for (int i = 0; i < 2; i++) {
                Grid g = startGrid(i);

                assert "true".equals(g.cluster().localNode().attribute(PREFER_IPV4));
            }

            System.setProperty(PREFER_IPV4, "false");

            startGrid(2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentDeploymentModes() throws Exception {
        try {
            startGrid(1);

            mode = CONTINUOUS;

            try {
                startGrid(2);

                fail();
            }
            catch (GridException e) {
                assertTrue(e.getCause().getMessage().startsWith("Remote node has deployment mode different from"));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentPeerClassLoadingEnabledFlag() throws Exception {
        try {
            startGrid(1);

            p2pEnabled = true;

            try {
                startGrid(2);

                fail();
            }
            catch (GridException e) {
                assertTrue(e.getCause().getMessage().startsWith("Remote node has peer class loading enabled flag " +
                    "different from"));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentDataCenterIds() throws Exception {
        try {
            GridConfiguration cfg1 = getConfiguration(getTestGridName(1));

            cfg1.setDataCenterId((byte)1);

            G.start(cfg1);

            try {
                GridConfiguration cfg2 = getConfiguration(getTestGridName(2));

                cfg2.setDataCenterId((byte)2);

                G.start(cfg2);

                fail();
            }
            catch (GridException e) {
                assertTrue(e.getCause().getMessage().startsWith("Remote node has data center ID different from"));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param preferIpV4 {@code java.net.preferIPv4Stack} system property value.
     * @throws Exception If failed.
     */
    private void testPreferIpV4Stack(boolean preferIpV4) throws Exception {
        try {
            String val = String.valueOf(preferIpV4);

            System.setProperty(PREFER_IPV4, val);

            for (int i = 0; i < 2; i++) {
                Grid g = startGrid(i);

                assert val.equals(g.cluster().localNode().attribute(PREFER_IPV4));
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
