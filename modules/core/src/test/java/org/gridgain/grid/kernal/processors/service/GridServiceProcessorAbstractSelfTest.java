/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Service Processor")
public abstract class GridServiceProcessorAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Random generator. */
    private static final Random RAND = new Random();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        GridServiceConfiguration[] svcs = services();

        if (svcs != null)
            cfg.setServiceConfiguration(services());

        return cfg;
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
    public void testDuplicateName() throws Exception {
        String name = "dupService";

        GridFuture<?> fut1 = randomGrid().services().deploySingleton(name, new DummyService());
        GridFuture<?> fut2 = randomGrid().services().deploySingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut1.get();

        info("Finished waiting for service future: " + name);

        try {
            fut2.get();

            fail("Failed to receive duplicate service exception.");
        }
        catch (GridException e) {
            info("Received duplicate service exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNode() throws Exception {
        Grid g = randomGrid();

        String name = "serviceOnEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deployOnEachNode(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(nodeCount(), DummyService.started());
        assertFalse(DummyService.isCancelled());

        checkCount(name, g.services().deployedServices(), nodeCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploySingleton() throws Exception {
        Grid g = randomGrid();

        String name = "serviceSingleton";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deploySingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(1, DummyService.started());
        assertFalse(DummyService.isCancelled());

        checkCount(name, g.services().deployedServices(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployMultiple1() throws Exception {
        Grid g = randomGrid();

        String name = "serviceMultiple1";

        CountDownLatch latch = new CountDownLatch(nodeCount() * 2);

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(nodeCount() * 2, DummyService.started());
        assertFalse(DummyService.isCancelled());

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

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deployMultiple(name, new DummyService(), cnt, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(cnt, DummyService.started());
        assertFalse(DummyService.isCancelled());

        checkCount(name, g.services().deployedServices(), cnt);
    }

    /**
     * @param svcName Service name.
     * @param descs Descriptors.
     * @param cnt Expected count
     */
    protected void checkCount(String svcName, Collection<GridServiceDescriptor> descs, int cnt) {
        int sum = 0;

        for (GridServiceDescriptor d : descs) {
            if (d.name().equals(svcName)) {
                for (Integer i : d.topologySnapshot().values())
                    sum += i;
            }
        }

        assertEquals(cnt, sum);
    }
}
