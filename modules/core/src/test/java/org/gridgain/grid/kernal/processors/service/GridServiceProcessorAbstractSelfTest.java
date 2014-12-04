/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        GridServiceConfiguration[] svcs = services();

        if (svcs != null)
            c.setServiceConfiguration(svcs);

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
    @SuppressWarnings("ConstantConditions")
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
    protected Ignite randomGrid() {
        return grid(RAND.nextInt(nodeCount()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameConfiguration() throws Exception {
        String name = "dupService";

        GridServices svcs1 = randomGrid().services().enableAsync();
        GridServices svcs2 = randomGrid().services().enableAsync();

        svcs1.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut1 = svcs1.future();

        svcs2.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut2 = svcs2.future();

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

        GridServices svcs1 = randomGrid().services().enableAsync();
        GridServices svcs2 = randomGrid().services().enableAsync();

        svcs1.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut1 = svcs1.future();

        svcs2.deployNodeSingleton(name, new DummyService());

        IgniteFuture<?> fut2 = svcs2.future();

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

        Ignite g = randomGrid();

        g.services().deployNodeSingleton(name, new DummyService());

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

        Ignite g = randomGrid();

        g.services().deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

        GridTestUtils.retryAssert(log, 50, 200, new CA() {
            @Override
            public void apply() {
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
        Ignite g = randomGrid();

        String name = "serviceOnEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        GridServices svcs = g.services().enableAsync();

        svcs.deployNodeSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

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
        Ignite g = randomGrid();

        String name = "serviceSingleton";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        GridServices svcs = g.services().enableAsync();

        svcs.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

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
        Ignite g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        String name = "serviceAffinity";

        GridServices svcs = g.services().enableAsync();

        svcs.deployKeyAffinitySingleton(name, new AffinityService(affKey),
                CACHE_NAME, affKey);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().deployedServices(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployMultiple1() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceMultiple1";

        CountDownLatch latch = new CountDownLatch(nodeCount() * 2);

        DummyService.exeLatch(name, latch);

        GridServices svcs = g.services().enableAsync();

        svcs.deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

        IgniteFuture<?> fut = svcs.future();

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
        Ignite g = randomGrid();

        String name = "serviceMultiple2";

        int cnt = nodeCount() * 2 + 1;

        CountDownLatch latch = new CountDownLatch(cnt);

        DummyService.exeLatch(name, latch);

        GridServices svcs = g.services().enableAsync();

        svcs.deployMultiple(name, new DummyService(), cnt, 3);

        IgniteFuture<?> fut = svcs.future();

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
        Ignite g = randomGrid();

        String name = "serviceCancel";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        g.services().deployClusterSingleton(name, new DummyService());

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(1);

        DummyService.cancelLatch(name, latch);

        g.services().cancel(name);

        info("Cancelled service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 1, DummyService.cancelled(name));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelEachNode() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceCancelEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        g.services().deployNodeSingleton(name, new DummyService());

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(nodeCount());

        DummyService.cancelLatch(name, latch);

        g.services().cancel(name);

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
     * @return Services count.
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
     * Affinity service.
     */
    protected static class AffinityService implements GridService {
        /** */
        private static final long serialVersionUID = 0L;

        /** Affinity key. */
        private final Object affKey;

        /** Grid. */
        @GridInstanceResource
        private Ignite g;

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
            X.println("Initializing affinity service for key: " + affKey);

            ClusterNode n = g.cache(CACHE_NAME).affinity().mapKeyToNode(affKey);

            assertNotNull(n);
            assertTrue(n.isLocal());
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) {
            X.println("Executing affinity service for key: " + affKey);
        }
    }

    /**
     * Counter service implementation.
     */
    protected static class CounterServiceImpl implements CounterService, GridService {
        /** Auto-injected grid instance. */
        @GridInstanceResource
        private Ignite ignite;

        /** */
        private GridCache<String, Value> cache;

        /** Cache key. */
        private String key;

        /** Invocation count. */
        private AtomicInteger locIncrements = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public int localIncrements() {
            return locIncrements.get();
        }

        /** {@inheritDoc} */
        @Override public int increment() {
            locIncrements.incrementAndGet();

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
            X.println("Stopping counter service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(GridServiceContext ctx) throws Exception {
            X.println("Initializing counter service: " + ctx.name());

            key = ctx.name();

            cache = ignite.cache(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) throws Exception {
            X.println("Executing counter service: " + ctx.name());
        }

        /**
         *
         */
        private static class Value implements Serializable {
            /** Value. */
            private final int v;

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

            /** {@inheritDoc} */
            @Override public int hashCode() {
                return v;
            }
        }
    }
}
