/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Service Processor")
public abstract class GridServiceProcessorAbstractSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "testServiceCache";

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Random generator. */
    private static final Random RAND = new Random();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        GridServiceConfiguration[] svcs = services();

        if (svcs != null)
            c.setServiceConfiguration(services());

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(GridCacheMode.PARTITIONED);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * Gets number of nodes.
     *
     * @return Number of nodes.
     */
    protected abstract int nodeCount();

    /**
     * Gets services configurations.
     *
     * @return Services configuration.
     */
    protected GridServiceConfiguration[] services() {
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert nodeCount() >= 1;

        for (int i = 0; i < nodeCount(); i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        DummyService.reset();
    }

    /**
     * @return Random grid.
     */
    protected Grid randomGrid() {
        return grid(RAND.nextInt(nodeCount()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameConfiguration() throws Exception {
        String name = "dupService";

        GridFuture<?> fut1 = randomGrid().services().deployClusterSingleton(name, new DummyService());
        GridFuture<?> fut2 = randomGrid().services().deployClusterSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut1.get();

        info("Finished waiting for service future1: " + name);

        // This must succeed without exception because configuration is the same.
        fut2.get();

        info("Finished waiting for service future2: " + name);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentConfiguration() throws Exception {
        String name = "dupService";

        GridFuture<?> fut1 = randomGrid().services().deployClusterSingleton(name, new DummyService());
        GridFuture<?> fut2 = randomGrid().services().deployNodeSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut1.get();

        info("Finished waiting for service future: " + name);

        try {
            fut2.get();

            fail("Failed to receive mismatching configuration exception.");
        }
        catch (GridException e) {
            info("Received mismatching configuration exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetServiceByName() throws Exception {
        String name = "serviceByName";

        Grid g = randomGrid();

        g.services().deployNodeSingleton(name, new DummyService()).get();

        DummyService srvc = g.services().service(name);

        assertNotNull(srvc);

        Collection<DummyService> srvcs = g.services().services(name);

        assertEquals(1, srvcs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetServicesByName() throws Exception {
        final String name = "servicesByName";

        Grid g = randomGrid();

        g.services().deployMultiple(name, new DummyService(), nodeCount() * 2, 3).get();

        GridTestUtils.retryAssert(log, 50, 200, new CA() {
            @Override public void apply() {
                int cnt = 0;

                for (int i = 0; i < nodeCount(); i++) {
                    Collection<DummyService> srvcs = grid(i).services().services(name);

                    if (srvcs != null)
                        cnt += srvcs.size();
                }

                assertEquals(nodeCount() * 2, cnt);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNode() throws Exception {
        Grid g = randomGrid();

        String name = "serviceOnEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployNodeSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().deployedServices(), nodeCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploySingleton() throws Exception {
        Grid g = randomGrid();

        String name = "serviceSingleton";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployClusterSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().deployedServices(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityDeploy() throws Exception {
        Grid g = randomGrid();

        String name = "serviceAffinity";

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        final CountDownLatch latch = new CountDownLatch(1);

        GridFuture<?> fut = g.services().deployKeyAffinitySingleton(name, new AffinityService(latch, affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        checkCount(name, g.services().deployedServices(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployMultiple1() throws Exception {
        Grid g = randomGrid();

        String name = "serviceMultiple1";

        CountDownLatch latch = new CountDownLatch(nodeCount() * 2);

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount() * 2, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().deployedServices(), nodeCount() * 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployMultiple2() throws Exception {
        Grid g = randomGrid();

        String name = "serviceMultiple2";

        int cnt = nodeCount() * 2 + 1;

        CountDownLatch latch = new CountDownLatch(cnt);

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployMultiple(name, new DummyService(), cnt, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, cnt, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().deployedServices(), cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelSingleton() throws Exception {
        Grid g = randomGrid();

        String name = "serviceCancel";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        g.services().deployClusterSingleton(name, new DummyService()).get();

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(1);

        DummyService.cancelLatch(name, latch);

        g.services().cancel(name).get();

        info("Cancelled service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 1, DummyService.cancelled(name));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelEachNode() throws Exception {
        Grid g = randomGrid();

        String name = "serviceCancelEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        g.services().deployNodeSingleton(name, new DummyService()).get();

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(nodeCount());

        DummyService.cancelLatch(name, latch);

        g.services().cancel(name).get();

        info("Cancelled service: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, nodeCount(), DummyService.cancelled(name));
    }

    /**
     * @throws Exception If failed.
     */
    protected void startExtraNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startGrid(nodeCount() + i);
    }

    /**
     * @throws Exception If failed.
     */
    protected void stopExtraNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            stopGrid(nodeCount() + i);
    }

    /**
     * @param svcName Service name.
     * @param descs Descriptors.
     * @param cnt Expected count.
     */
    protected void checkCount(String svcName, Iterable<GridServiceDescriptor> descs, int cnt) {
        assertEquals(cnt, actualCount(svcName, descs));
    }

    /**
     * @param svcName Service name.
     * @param descs Descriptors.
     */
    protected int actualCount(String svcName, Iterable<GridServiceDescriptor> descs) {
        int sum = 0;

        for (GridServiceDescriptor d : descs) {
            if (d.name().equals(svcName)) {
                for (Integer i : d.topologySnapshot().values())
                    sum += i;
            }
        }

        return sum;
    }

    /**
     * Affinity service.
     */
    protected static class AffinityService implements GridService {
        /** */
        private static final long serialVersionUID = 0L;

        /** Latch. */
        private static CountDownLatch latch;

        /** Affinity key. */
        private final Object affKey;

        @GridInstanceResource
        private Grid g;

        /**
         * @param latch Latch.
         * @param affKey Affinity key.
         */
        public AffinityService(CountDownLatch latch, Object affKey) {
            AffinityService.latch = latch;

            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) {
            System.out.println("Executing affinity service for key: " + affKey);

            GridNode n = g.cache(CACHE_NAME).affinity().mapKeyToNode(affKey);

            assertNotNull(n);
            assertTrue(n.isLocal());

            latch.countDown();
        }
    }
}
