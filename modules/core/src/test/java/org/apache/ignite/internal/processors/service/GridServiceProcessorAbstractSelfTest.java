/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Service Processor")
public abstract class GridServiceProcessorAbstractSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "testServiceCache";

    /** Random generator. */
    private static final Random RAND = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        ServiceConfiguration[] svcs = services();

        if (svcs != null)
            c.setServiceConfiguration(svcs);

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cc.setName(CACHE_NAME);
        cc.setCacheMode(CacheMode.PARTITIONED);
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
    protected ServiceConfiguration[] services() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assert nodeCount() >= 1;

        for (int i = 0; i < nodeCount(); i++)
            startGrid(i);
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

    /** */
    protected void startExtraNodes(int servers, int clients) throws Exception {
        startExtraNodes(servers);

        for (int i = 0; i < clients; i++) {
            final String nodeName = getTestIgniteInstanceName(nodeCount() + servers + i);

            startGrid(nodeName, getConfiguration(nodeName).setClientMode(true));
        }
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
    protected IgniteEx randomGrid() {
        return grid(RAND.nextInt(nodeCount()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSameConfigurationOld() throws Exception {
        String name = "dupServiceOld";

        IgniteServices svcs1 = randomGrid().services().withAsync();
        IgniteServices svcs2 = randomGrid().services().withAsync();

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
    @Test
    public void testSameConfiguration() throws Exception {
        String name = "dupServiceOld";

        IgniteServices svcs1 = randomGrid().services();
        IgniteServices svcs2 = randomGrid().services();

        IgniteFuture<?> fut1 = svcs1.deployClusterSingletonAsync(name, new DummyService());

        IgniteFuture<?> fut2 = svcs2.deployClusterSingletonAsync(name, new DummyService());

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
    @Test
    public void testDifferentConfigurationOld() throws Exception {
        String name = "dupServiceOld";

        IgniteServices svcs1 = randomGrid().services().withAsync();
        IgniteServices svcs2 = randomGrid().services().withAsync();

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
        catch (IgniteException e) {
            info("Received mismatching configuration exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDifferentConfiguration() throws Exception {
        String name = "dupService";

        IgniteServices svcs1 = randomGrid().services();
        IgniteServices svcs2 = randomGrid().services();

        IgniteFuture<?> fut1 = svcs1.deployClusterSingletonAsync(name, new DummyService());

        IgniteFuture<?> fut2 = svcs2.deployNodeSingletonAsync(name, new DummyService());

        info("Deployed service: " + name);

        try {
            fut1.get();

            info("Finished waiting for service future: " + name);

            fut2.get();

            fail("Failed to receive mismatching configuration exception.");
        }
        catch (IgniteException e) {
            info("Received mismatching configuration exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testGetServicesByName() throws Exception {
        final String name = "servicesByName";

        Ignite g = randomGrid();

        g.services().deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

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
    @Test
    public void testDeployOnEachNodeOld() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceOnEachNodeOld";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployNodeSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), nodeCount());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployOnEachNode() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceOnEachNode";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        IgniteFuture<?> fut = g.services().deployNodeSingletonAsync(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), nodeCount());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeploySingletonOld() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceSingletonOld";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeploySingleton() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceSingleton";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        IgniteFuture<?> fut = g.services().deployClusterSingletonAsync(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityDeployOld() throws Exception {
        Ignite g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        String name = "serviceAffinityOld";

        IgniteServices svcs = g.services().withAsync();

        svcs.deployKeyAffinitySingleton(name, new AffinityService(affKey),
                CACHE_NAME, affKey);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().serviceDescriptors(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityDeploy() throws Exception {
        Ignite g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        String name = "serviceAffinity";

        IgniteFuture<?> fut = g.services().deployKeyAffinitySingletonAsync(name, new AffinityService(affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().serviceDescriptors(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployMultiple1Old() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceMultiple1Old";

        CountDownLatch latch = new CountDownLatch(nodeCount() * 2);

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployMultiple(name, new DummyService(), nodeCount() * 2, 3);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount() * 2, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), nodeCount() * 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployMultiple1() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceMultiple1";

        CountDownLatch latch = new CountDownLatch(nodeCount() * 2);

        DummyService.exeLatch(name, latch);

        IgniteFuture<?> fut = g.services().deployMultipleAsync(name, new DummyService(), nodeCount() * 2, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount() * 2, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), nodeCount() * 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployMultiple2Old() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceMultiple2Old";

        int cnt = nodeCount() * 2 + 1;

        CountDownLatch latch = new CountDownLatch(cnt);

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployMultiple(name, new DummyService(), cnt, 3);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, cnt, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployMultiple2() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceMultiple2";

        int cnt = nodeCount() * 2 + 1;

        CountDownLatch latch = new CountDownLatch(cnt);

        DummyService.exeLatch(name, latch);

        IgniteFuture<?> fut = g.services().deployMultipleAsync(name, new DummyService(), cnt, 3);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, cnt, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        checkCount(name, g.services().serviceDescriptors(), cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testCancelSingletonAsync() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceCancelAsync";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        g.services().deployClusterSingleton(name, new DummyService());

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(1);

        DummyService.cancelLatch(name, latch);

        g.services().cancelAsync(name).get();

        info("Cancelled service: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 1, DummyService.cancelled(name));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAsyncEachNode() throws Exception {
        Ignite g = randomGrid();

        String name = "serviceCancelEachNodeAsync";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        g.services().deployNodeSingleton(name, new DummyService());

        info("Deployed service: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        latch = new CountDownLatch(nodeCount());

        DummyService.cancelLatch(name, latch);

        g.services().cancelAsync(name).get();

        info("Cancelled service: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, nodeCount(), DummyService.cancelled(name));
    }

    /**
     * @param svcName Service name.
     * @param ignite Ignite instance.
     * @param cnt Expected count.
     */
    protected void checkCount(String svcName, IgniteEx ignite, int cnt) throws IgniteInterruptedCheckedException {
        AffinityTopologyVersion topVer = ignite.context().discovery().topologyVersionEx();

        waitForServicesReadyTopology(ignite, topVer);

        assertEquals(cnt, actualCount(svcName, ignite.services().serviceDescriptors()));
    }

    /**
     * @param svcName Service name.
     * @param descs Descriptors.
     * @param cnt Expected count.
     */
    protected void checkCount(String svcName, Iterable<ServiceDescriptor> descs, int cnt) {
        assertEquals(cnt, actualCount(svcName, descs));
    }

    /**
     * @param svcName Service name.
     * @param descs Descriptors.
     * @return Services count.
     */
    protected int actualCount(String svcName, Iterable<ServiceDescriptor> descs) {
        int sum = 0;

        for (ServiceDescriptor d : descs) {
            if (d.name().equals(svcName)) {
                for (Integer i : d.topologySnapshot().values())
                    sum += i;
            }
        }

        return sum;
    }

    /**
     * @param srvcName Service name
     * @param expectedDeps Expected number of service deployments
     *
     */
    protected boolean waitForDeployment(final String srvcName, final int expectedDeps) throws IgniteInterruptedCheckedException {
        final Ignite g = randomGrid();

        return GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() {
                return actualCount(srvcName, g.services().serviceDescriptors())  == expectedDeps;
            }
        }, 12_000);
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
     * @param ignite Ignite instance.
     * @param srvcName Affinity service name.
     */
    protected void checkAffinityServiceDeployment(Ignite ignite, String srvcName) {
        ServiceDescriptor desc = null;

        for (ServiceDescriptor d : ignite.services().serviceDescriptors()) {
            if (d.name().equals(srvcName)) {
                desc = d;

                break;
            }
        }

        assertNotNull(desc);

        assertEquals(1, desc.topologySnapshot().size());

        ClusterNode n = ignite.affinity(desc.cacheName()).mapKeyToNode(desc.affinityKey());

        assertNotNull(n);

        assertTrue(desc.topologySnapshot().containsKey(n.id()));
    }

    /**
     * Affinity service.
     */
    protected static class AffinityService implements Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** Affinity key. */
        private final Object affKey;

        /** Grid. */
        @IgniteInstanceResource
        private Ignite g;

        /**
         * @param affKey Affinity key.
         */
        public AffinityService(Object affKey) {
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            X.println("Initializing affinity service for key: " + affKey);
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            X.println("Executing affinity service for key: " + affKey);
        }
    }

    /**
     * Counter service implementation.
     */
    protected static class CounterServiceImpl implements CounterService, Service {
        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteCache<String, Value> cache;

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
                        Value old = cache.getAndPutIfAbsent(key, val = new Value(0));

                        if (old != null)
                            val = old;
                    }

                    Value newVal = new Value(val.get() + 1);

                    if (cache.replace(key, val, newVal))
                        return newVal.get();
                }

            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int get() {
            try {
                Value val = cache.get(key);

                return val == null ? 0 : val.get();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            X.println("Stopping counter service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            X.println("Initializing counter service: " + ctx.name());

            key = ctx.name();

            cache = ignite.cache(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
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
