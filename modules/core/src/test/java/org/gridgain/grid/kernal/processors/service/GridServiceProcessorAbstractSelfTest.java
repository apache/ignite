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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
        cc.setBackups(nodeCount());

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

        DummyService svc = g.services().service(name);

        assertNotNull(svc);

        Collection<DummyService> svcs = g.services().services(name);

        assertEquals(1, svcs.size());
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
                    Collection<DummyService> svcs = grid(i).services().services(name);

                    if (svcs != null)
                        cnt += svcs.size();
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

        GridFuture<?> fut = g.services().deployKeyAffinitySingleton(name, new AffinityService(affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

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

        /** Affinity key. */
        private final Object affKey;

        @GridInstanceResource
        private Grid g;

        /**
         * @param affKey Affinity key.
         */
        public AffinityService(Object affKey) {
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(GridServiceContext ctx) throws Exception {
            System.out.println("Initializing affinity service for key: " + affKey);

            GridNode n = g.cache(CACHE_NAME).affinity().mapKeyToNode(affKey);

            assertNotNull(n);
            assertTrue(n.isLocal());
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) {
            System.out.println("Executing affinity service for key: " + affKey);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterSingletonProxy() throws Exception {
        String name = "testClusterSingletonProxy";

        Grid grid = randomGrid();

        grid.services().deployClusterSingleton(name, new CounterServiceImpl()).get();

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonProxy() throws Exception {
        String name = "testNodeSingletonProxy";

        Grid grid = randomGrid();

        grid.services().deployNodeSingleton(name, new CounterServiceImpl()).get();

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
        assertEquals(10, svc.localIncrements());
        assertEquals(10, grid.forLocal().services().serviceProxy(name, CounterService.class, false).localIncrements());

        // Make sure that remote proxies were not called.
        for (GridNode n : grid.forRemotes().nodes()) {
            CounterService rmtSvc = grid.forNode(n).services().serviceProxy(name, CounterService.class, false);

            assertEquals(0, rmtSvc.localIncrements());
        }
    }

    /**
     * Counter service.
     */
    protected interface CounterService {
        /**
         * @return Number of increments happened on the same service instance.
         */
        int localIncrements();

        /**
         * @return Incremented value.
         */
        int increment();

        /**
         * @return Current value.
         */
        int get();
    }

    /**
     * Counter service implementation.
     */
    protected static class CounterServiceImpl implements CounterService, GridService {
        /** Auto-injected grid instance. */
        @GridInstanceResource
        private Grid grid;

        /** */
        private GridCache<String, Value> cache;

        /** Cache key. */
        private String key;

        /** Invocation count. */
        private AtomicInteger locInrements = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public int localIncrements() {
            return locInrements.get();
        }

        /** {@inheritDoc} */
        @Override public int increment() {
            locInrements.incrementAndGet();

            try {
                while (true) {
                    Value val = cache.get(key);

                    if (val == null) {
                        Value old = cache.putIfAbsent(key, val = new Value(0));

                        if (old != null)
                            val = old;
                    }

                    Value newVal = new Value(val.get() + 1);

                    if (cache.replace(key, val, newVal))
                        return newVal.get();
                }

            }
            catch (Exception e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int get() {
            try {
                Value val = cache.get(key);

                return val == null ? 0 : val.get();
            }
            catch (Exception e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            System.out.println("Stopping counter service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(GridServiceContext ctx) throws Exception {
            System.out.println("Initializing counter service: " + ctx.name());

            key = ctx.name();

            cache = grid.cache(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) throws Exception {
            System.out.println("Executing counter service: " + ctx.name());
        }

        /**
         *
         */
        private static class Value implements Serializable {
            /** */
            private int v;

            /**
             * @param v Value.
             */
            private Value(int v) {
                this.v = v;
            }

            /**
             * @return Value.
             */
            int get() {
                return v;
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                return this == o || o instanceof Value && v == ((Value)o).v;
            }
        }
    }
}
