/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test grids starting with non compatible release types.
 */
public class GridReleaseTypeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Counter. */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        final int idx = cnt.getAndIncrement();

        // Override node attributes in discovery spi.
        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, GridProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                if (idx == 0)
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, "edition-ent-1.0.0");
                else
                    attrs.put(GridNodeAttributes.ATTR_BUILD_VER, "edition-os-1.0.0");
            }
        };

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testErrorDuringStartingWithDifferentReleaseTypes() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    startGrids(2);
                }
                finally {
                    stopAllGrids();
                }

                return null;
            }
        }, GridException.class, null);
    }
}
