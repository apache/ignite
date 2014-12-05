/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.configuration.*;
import org.apache.ignite.product.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test grids starting with non compatible release types.
 */
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Counter. */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** */
    private String firstNodeVer;

    /** */
    private String secondNodeVer;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        final int idx = cnt.getAndIncrement();

        // Override node attributes in discovery spi.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                if (idx % 2 == 0)
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, firstNodeVer);
                else
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, secondNodeVer);
            }
        };

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinTopologyWithDifferentReleaseType() throws Exception {
        firstNodeVer = "1.0.0-ent";
        secondNodeVer = "1.0.0-os";

        try {
            startGrids(2);
        }
        catch (GridException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            assertTrue(
                "Caught exception does not contain specified string.",
                stackTrace.contains("Topology cannot contain nodes of both enterprise and open source")
            );

            return;
        }
        finally {
            stopAllGrids();
        }

        fail("Exception has not been thrown.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testOsEditionDoesNotSupportRollingUpdates() throws Exception {
        firstNodeVer = "1.0.0-os";
        secondNodeVer = "1.0.1-os";

        try {
            startGrids(2);
        }
        catch (GridException e) {
            StringWriter errors = new StringWriter();

            e.printStackTrace(new PrintWriter(errors));

            String stackTrace = errors.toString();

            assertTrue(
                "Caught exception does not contain specified string.",
                stackTrace.contains("Local node and remote node have different version numbers")
            );

            return;
        }
        finally {
            stopAllGrids();
        }

        fail("Exception has not been thrown.");
    }
}
