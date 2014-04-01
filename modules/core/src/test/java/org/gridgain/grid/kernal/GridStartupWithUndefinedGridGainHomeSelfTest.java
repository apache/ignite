/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal;

import junit.framework.*;
import org.apache.log4j.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;

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
    public static final int COUNT = 2;

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithUndefinedGridGainHome() throws Exception {
        // We can't use U.getGridGainHome() here because
        // it will initialize cached value which is forbidden to override.
        String ggHome = X.getSystemOrEnv(GridSystemProperties.GG_HOME);

        assert ggHome != null;

        U.setGridGainHome(null);

        String ggHome0 = U.getGridGainHome();

        assert ggHome0 == null;

        GridLogger log = new GridLog4jLogger(Logger.getRootLogger());

        log.info(">>> Test started: " + getName());
        log.info("Grid start-stop test count: " + COUNT);

        for (int i = 0; i < COUNT; i++) {
            GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

            disc.setIpFinder(IP_FINDER);

            GridConfiguration cfg = new GridConfiguration();

            // We have to explicitly configure paths to license and jetty configs because of undefined GRIDGAIN_HOME.
            cfg.setLicenseUrl("file:///" + ggHome + "/" + GridGain.DFLT_LIC_FILE_NAME);
            cfg.setRestJettyPath(ggHome + File.separator + "modules/core/src/test/config/jetty/rest-jetty.xml");
            // Default console logger is used
            cfg.setGridLogger(log);
            cfg.setDiscoverySpi(disc);

            try (Grid g = G.start(cfg)) {
                assert g != null;

                ggHome0 = U.getGridGainHome();

                assert ggHome0 == null;

                X.println("Stopping grid " + g.localNode().id());
            }
        }
    }
}
