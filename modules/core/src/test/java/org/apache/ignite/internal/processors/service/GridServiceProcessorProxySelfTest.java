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

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Service proxy test.
 */
public class GridServiceProcessorProxySelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).services().cancelAll();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingletonProxy() throws Exception {
        String name = "testNodeSingletonProxy";

        Ignite ignite = randomGrid();

        ignite.services().deployNodeSingleton(name, new CounterServiceImpl());

        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
        assertEquals(10, svc.localIncrements());
        assertEquals(10, ignite.services(ignite.cluster().forLocal()).
            serviceProxy(name, CounterService.class, false).localIncrements());

        // Make sure that remote proxies were not called.
        for (ClusterNode n : ignite.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    ignite.services(ignite.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

            assertEquals(0, rmtSvc.localIncrements());
        }
    }

    /**
     * Unwraps error message from InvocationTargetException.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testException() throws Exception {
        String name = "errorService";

        Ignite ignite = grid(0);

        ignite.services(ignite.cluster().forRemotes()).deployNodeSingleton(name, new ErrorServiceImpl());

        final ErrorService svc = ignite.services().serviceProxy(name, ErrorService.class, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                svc.go();

                return null;
            }
        }, ErrorServiceException.class, "Test exception");

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterSingletonProxy() throws Exception {
        String name = "testClusterSingletonProxy";

        Ignite ignite = randomGrid();

        ignite.services().deployClusterSingleton(name, new CounterServiceImpl());

        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiNodeProxy() throws Exception {
        Ignite ignite = randomGrid();

        int extras = 3;

        startExtraNodes(extras);

        String name = "testMultiNodeProxy";

        ignite.services().deployNodeSingleton(name, new CounterServiceImpl());

        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < extras; i++) {
            svc.increment();

            stopGrid(nodeCount() + i);
        }

        assertEquals(extras, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingletonRemoteNotStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteNotStickyProxy";

        Ignite ignite = randomGrid();

        // Deploy only on remote nodes.
        ignite.services(ignite.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        info("Deployed service: " + name);

        // Get local proxy.
        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (ClusterNode n : ignite.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    ignite.services(ignite.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

            int cnt = rmtSvc.localIncrements();

            // Since deployment is not stick, count on each node must be less than 10.
            assertTrue("Invalid local increments: " + cnt, cnt != 10);

            total += cnt;
        }

        assertEquals(10, total);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingletonRemoteStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteStickyProxy";

        Ignite ignite = randomGrid();

        // Deploy only on remote nodes.
        ignite.services(ignite.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        // Get local proxy.
        CounterService svc = ignite.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (ClusterNode n : ignite.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    ignite.services(ignite.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

            int cnt = rmtSvc.localIncrements();

            assertTrue("Invalid local increments: " + cnt, cnt == 10 || cnt == 0);

            total += rmtSvc.localIncrements();
        }

        assertEquals(10, total);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingletonProxyInvocation() throws Exception {
        final String name = "testProxyInvocationFromSeveralNodes";

        final Ignite ignite = grid(0);

        ignite.services(ignite.cluster().forLocal()).deployClusterSingleton(name, new MapServiceImpl<String, Integer>());

        for (int i = 1; i < nodeCount(); i++) {
            MapService<Integer, String> svc = grid(i).services()
                .serviceProxy(name, MapService.class, false, 1_000L);

            // Make sure service is a proxy.
            assertFalse(svc instanceof Service);

            svc.put(i, Integer.toString(i));
        }

        assertEquals(nodeCount() - 1, ignite.services().serviceProxy(name, MapService.class, false).size());
    }

    /**
     * Checks local service without the statistics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLocalProxyInvocationWithoutStat() throws Exception {
        checkLocalProxy(false);
    }

    /**
     * Checks local service with the statistics enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLocalProxyInvocationWithStat() throws Exception {
        checkLocalProxy(true);
    }

    /**
     * Checks remote non-sticky proxy without the statistics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteNotStickProxyInvocationWithoutStat() throws Exception {
        checkRemoteProxy(false, false);
    }

    /**
     * Checks remote non-sticky proxy with the statistics enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteNotStickyProxyInvocationWithStat() throws Exception {
        checkRemoteProxy(true, false);
    }

    /**
     * Checks remote sticky proxy without the statistics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteStickyProxyInvocationWithoutStat() throws Exception {
        checkRemoteProxy(false, true);
    }

    /**
     * Checks remote sticky proxy with the statistics enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteStickyProxyInvocationWithStat() throws Exception {
        checkRemoteProxy(true, true);
    }

    /**
     * Checks remote service proxy (node singleton) with or without the statistics.
     *
     * @param withStat If {@code true}, enables the service metrics, {@link ServiceConfiguration#setStatisticsEnabled(boolean)}.
     * @param sticky If {@code true}, requests sticky proxy.
     */
    private void checkRemoteProxy(boolean withStat, boolean sticky) throws InterruptedException {
        final String svcName = "remoteServiceTest";

        deployNodeSingleton(svcName, withStat);

        Ignite ignite = grid(0);

        // Get remote proxy.
        MapService<Integer, String> svc = ignite.services(ignite.cluster().forRemotes()).
            serviceProxy(svcName, MapService.class, sticky);

        assertFalse(svc instanceof Service);

        assertTrue(Arrays.asList(svc.getClass().getInterfaces()).contains(MapService.class));

        assertEquals(svc.size(), 0);

        for (int i = 0; i < nodeCount(); i++)
            svc.put(i, Integer.toString(i));

        int size = 0;

        for (ClusterNode n : ignite.cluster().forRemotes().nodes()) {
            MapService<Integer, String> map = ignite.services(ignite.cluster().forNode(n)).
                serviceProxy(svcName, MapService.class, sticky);

            assertFalse(map instanceof Service);

            assertTrue(Arrays.asList(svc.getClass().getInterfaces()).contains(MapService.class));

            if (map.size() != 0)
                size += map.size();
        }

        assertEquals(nodeCount(), size);
    }

    /**
     * Checks local service (node singleton) with or without statistics.
     *
     * @param withStat If {@code true}, enables the service metrics, {@link ServiceConfiguration#setStatisticsEnabled(boolean)}.
     */
    private void checkLocalProxy(boolean withStat) throws Exception {
        final String svcName = "localProxyTest";

        deployNodeSingleton(svcName, withStat);

        for (int i = 0; i < nodeCount(); i++) {
            final int idx = i;

            final AtomicReference<MapService<Integer, String>> ref = new AtomicReference<>();

            //wait because after deployNodeSingleton we don't have guarantees what service was deploy.
            boolean wait = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    MapService<Integer, String> svc = grid(idx)
                        .services()
                        .serviceProxy(svcName, MapService.class, false);

                    ref.set(svc);

                    return (Proxy.isProxyClass(svc.getClass())) &&
                        Arrays.asList(svc.getClass().getInterfaces()).contains(MapService.class);
                }
            }, 2000);

            // Make sure service is a local instance.
            assertTrue("Invalid service instance [srv=" + ref.get() + ", node=" + i + ']', wait);

            ref.get().put(i, Integer.toString(i));
        }

        MapService<Integer, String> map = grid(0).services().serviceProxy(svcName, MapService.class, false);

        for (int i = 0; i < nodeCount(); i++)
            assertEquals(1, map.size());
    }

    /**
     * Deploys {@link MapServiceImpl} service over the cluster as node singleton.
     *
     * @param svcName Service name
     * @param withStat If {@code true}, enabled the serive metrics {@link ServiceConfiguration#setStatisticsEnabled(boolean)}.
     */
    private void deployNodeSingleton(String svcName, boolean withStat) throws InterruptedException {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName(svcName);
        svcCfg.setMaxPerNodeCount(1);
        svcCfg.setTotalCount(nodeCount());
        svcCfg.setService(new MapServiceImpl<String, Integer>());
        svcCfg.setStatisticsEnabled(withStat);

        grid(0).services().deploy(svcCfg);

        awaitPartitionMapExchange();
    }

    /**
     * Simple map service.
     *
     * @param <K> Type of cache keys.
     * @param <V> Type of cache values.
     */
    protected interface MapService<K, V> {
        /**
         * Puts key-value pair into map.
         *
         * @param key Key.
         * @param val Value.
         */
        void put(K key, V val);

        /**
         * Gets value based on key.
         *
         * @param key Key.
         * @return Value.
         */
        V get(K key);

        /**
         * Clears map.
         */
        void clear();

        /**
         * @return Map size.
         */
        int size();
    }

    /**
     * Cache service implementation.
     */
    protected static class MapServiceImpl<K, V> implements MapService<K, V>, Service {
        /** Underlying cache map. */
        private final Map<K, V> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            map.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            map.clear();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return map.size();
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            X.println("Stopping cache service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            X.println("Initializing counter service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            X.println("Executing cache service: " + ctx.name());
        }
    }

    /**
     *
     */
    protected interface ErrorService extends Service {
        /**
         *
         */
        void go() throws Exception;
    }

    /**
     *
     */
    protected static class ErrorServiceImpl implements ErrorService {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void go() throws Exception {
            throw new ErrorServiceException("Test exception");
        }
    }

    /** */
    private static class ErrorServiceException extends Exception {
        /** */
        ErrorServiceException(String msg) {
            super(msg);
        }
    }
}
