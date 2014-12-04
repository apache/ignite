/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests affinity and affinity mapper P2P loading.
 */
public class GridAffinityP2PSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String EXT_AFFINITY_MAPPER_CLS_NAME = "org.gridgain.grid.tests.p2p.GridExternalAffinityMapper";

    /** */
    private static final String EXT_AFFINITY_CLS_NAME = "org.gridgain.grid.tests.p2p.GridExternalAffinity";

    /** URL of classes. */
    private static final URL[] URLS;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /**
     * Initialize URLs.
     */
    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /**
     *
     */
    public GridAffinityP2PSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setDeploymentMode(depMode);

        if (gridName.endsWith("1"))
            c.setCacheConfiguration(); // Empty cache configuration.
        else {
            assert gridName.endsWith("2") || gridName.endsWith("3");

            GridCacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(URLS);

            cc.setAffinity((GridCacheAffinityFunction)ldr.loadClass(EXT_AFFINITY_CLS_NAME).newInstance());
            cc.setAffinityMapper((GridCacheAffinityKeyMapper)ldr.loadClass(EXT_AFFINITY_MAPPER_CLS_NAME)
                .newInstance());

            c.setCacheConfiguration(cc);
            c.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, gridName.endsWith("2") ? 0 : 1));
        }

        return c;
    }

    /**
     * Test {@link GridDeploymentMode#PRIVATE} mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        affinityTest();
    }

    /**
     * Test {@link GridDeploymentMode#ISOLATED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        depMode = GridDeploymentMode.ISOLATED;

        affinityTest();
    }

    /**
     * Test {@link GridDeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        affinityTest();
    }

    /**
     * Test {@link GridDeploymentMode#SHARED} mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        affinityTest();
    }

    /** @throws Exception If failed. */
    private void affinityTest() throws Exception {
        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);
        Ignite g3 = startGrid(3);

        try {
            assert g1.configuration().getCacheConfiguration().length == 0;
            assert g2.configuration().getCacheConfiguration()[0].getCacheMode() == PARTITIONED;
            assert g3.configuration().getCacheConfiguration()[0].getCacheMode() == PARTITIONED;

            GridNode first = g2.cluster().localNode();
            GridNode second = g3.cluster().localNode();

            //When external affinity and mapper are set to cache configuration we expect the following.
            //Key 0 is mapped to partition 0, first node.
            //Key 1 is mapped to partition 1, second node.
            //key 2 is mapped to partition 0, first node because mapper substitutes key 2 with affinity key 0.
            Map<GridNode, Collection<Integer>> map = g1.cluster().mapKeysToNodes(null, F.asList(0));

            assertNotNull(map);
            assertEquals("Invalid map size: " + map.size(), 1, map.size());
            assertEquals(F.first(map.keySet()), first);

            GridNode n1 = g1.cluster().mapKeyToNode(null, 1);

            assertNotNull(n1);

            UUID id1 = n1.id();

            assertNotNull(id1);
            assertEquals(second.id(), id1);

            GridNode n2 = g1.cluster().mapKeyToNode(null, 2);

            assertNotNull(n2);

            UUID id2 = n2.id();

            assertNotNull(id2);
            assertEquals(first.id(), id2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }
}
