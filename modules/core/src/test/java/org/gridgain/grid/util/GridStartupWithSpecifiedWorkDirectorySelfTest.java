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

import java.io.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.gridgain.grid.util.GridUtils.*;

/**
 * Checks creation of work folder.
 */
public class GridStartupWithSpecifiedWorkDirectorySelfTest extends TestCase {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_COUNT = 2;

    /** System temp directory. */
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        // Protection against previously cached values.
        nullifyHomeDirectory();
        nullifyWorkDirectory();
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        // Next grid in the same VM shouldn't use cached values produced by these tests.
        nullifyHomeDirectory();
        nullifyWorkDirectory();

        U.setWorkDirectory(null, U.getGridGainHome());
    }

    /**
     * @param log Grid logger.
     * @return Grid configuration.
     */
    private IgniteConfiguration getConfiguration(IgniteLogger log) {
        // We can't use U.getGridGainHome() here because
        // it will initialize cached value which is forbidden to override.
        String ggHome = IgniteSystemProperties.getString(GG_HOME);

        assert ggHome != null;

        U.setGridGainHome(null);

        String ggHome0 = U.getGridGainHome();

        assert ggHome0 == null;

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(IP_FINDER);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(log);
        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithUndefinedHomeAndWorkDirs() throws Exception {
        IgniteLogger log = new IgniteJavaLogger();

        log.info(">>> Test started: " + getName());
        log.info("Grid start-stop test count: " + GRID_COUNT);

        File testWorkDir = null;

        try {
            for (int i = 0; i < GRID_COUNT; i++) {
                try (Ignite g = G.start(getConfiguration(log))) {
                    assert g != null;

                    testWorkDir = U.resolveWorkDirectory(getName(), true);

                    assertTrue("Work directory wasn't created", testWorkDir.exists());

                    assertTrue("Work directory must be located in OS temp directory",
                        testWorkDir.getAbsolutePath().startsWith(TMP_DIR));

                    System.out.println(testWorkDir);

                    X.println("Stopping grid " + g.cluster().localNode().id());
                }
            }
        }
        finally {
            if (testWorkDir != null && testWorkDir.getAbsolutePath().startsWith(TMP_DIR))
                U.delete(testWorkDir);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithUndefinedHomeAndConfiguredWorkDirs() throws Exception {
        IgniteLogger log = new IgniteJavaLogger();

        log.info(">>> Test started: " + getName());
        log.info("Grid start-stop test count: " + GRID_COUNT);

        String tmpWorkDir = new File(TMP_DIR, getName() + "_" + UUID.randomUUID()).getAbsolutePath();

        try {
            for (int i = 0; i < GRID_COUNT; i++) {
                IgniteConfiguration cfg = getConfiguration(log);

                cfg.setWorkDirectory(tmpWorkDir);

                try (Ignite g = G.start(cfg)) {
                    assert g != null;

                    File testWorkDir = U.resolveWorkDirectory(getName(), true);

                    assertTrue("Work directory wasn't created", testWorkDir.exists());

                    assertTrue("Work directory must be located in configured directory",
                        testWorkDir.getAbsolutePath().startsWith(tmpWorkDir));

                    X.println("Stopping grid " + g.cluster().localNode().id());
                }
            }
        } finally {
            U.delete(new File(tmpWorkDir));
        }
    }
}
