/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.logger.java.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.gridgain.grid.util.GridUtils.*;

/**
 * Checks that node can be started without operations with undefined GRIDGAIN_HOME.
 * <p>
 * Notes:
 * 1. The test intentionally extends JUnit {@link TestCase} class to make the test
 * independent from {@link GridCommonAbstractTest} stuff.
 * 2. Do not replace native Java asserts with JUnit ones - test won't fall on TeamCity.
 */
public class GridStartupWithUndefinedGridGainHomeSelfTest extends TestCase {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        // Next grid in the same VM shouldn't use cached values produced by these tests.
        nullifyHomeDirectory();

        U.getGridGainHome();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithUndefinedGridGainHome() throws Exception {
        GridUtils.nullifyHomeDirectory();

        // We can't use U.getGridGainHome() here because
        // it will initialize cached value which is forbidden to override.
        String ggHome = IgniteSystemProperties.getString(GG_HOME);

        assert ggHome != null;

        U.setGridGainHome(null);

        String ggHome0 = U.getGridGainHome();

        assert ggHome0 == null;

        IgniteLogger log = new IgniteJavaLogger();

        log.info(">>> Test started: " + getName());
        log.info("Grid start-stop test count: " + GRID_COUNT);

        for (int i = 0; i < GRID_COUNT; i++) {
            GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

            disc.setIpFinder(IP_FINDER);

            IgniteConfiguration cfg = new IgniteConfiguration();

            // We have to explicitly configure path to license config because of undefined GRIDGAIN_HOME.
            cfg.setLicenseUrl("file:///" + ggHome + "/" + Ignition.DFLT_LIC_FILE_NAME);

            // Default console logger is used
            cfg.setGridLogger(log);
            cfg.setDiscoverySpi(disc);
            cfg.setRestEnabled(false);

            try (Ignite g = G.start(cfg)) {
                assert g != null;

                ggHome0 = U.getGridGainHome();

                assert ggHome0 == null;

                X.println("Stopping grid " + g.cluster().localNode().id());
            }
        }
    }
}
