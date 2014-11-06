/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Service proxy test.
 */
public class GridServiceProcessorProxySelfTest extends GridServiceProcessorAbstractSelfTest {

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonProxy() throws Exception {
        String name = "testNodeSingletonProxy";

        Grid grid = randomGrid();

        grid.services().deployNodeSingleton(name, new CounterServiceImpl());

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
        assertEquals(10, svc.localIncrements());
        assertEquals(10, grid.services(grid.cluster().forLocal()).
            serviceProxy(name, CounterService.class, false).localIncrements());

        // Make sure that remote proxies were not called.
        for (GridNode n : grid.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    grid.services(grid.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

            assertEquals(0, rmtSvc.localIncrements());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterSingletonProxy() throws Exception {
        String name = "testClusterSingletonProxy";

        Grid grid = randomGrid();

        grid.services().deployClusterSingleton(name, new CounterServiceImpl());

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiNodeProxy() throws Exception {
        Grid grid = randomGrid();

        int extras = 3;

        startExtraNodes(extras);

        String name = "testMultiNodeProxy";

        grid.services().deployNodeSingleton(name, new CounterServiceImpl());

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < extras; i++) {
            svc.increment();

            stopGrid(nodeCount() + i);
        }

        assertEquals(extras, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonRemoteNotStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteNotStickyProxy";

        Grid grid = randomGrid();

        // Deploy only on remote nodes.
        grid.services(grid.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        info("Deployed service: " + name);

        // Get local proxy.
        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (GridNode n : grid.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    grid.services(grid.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

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
    public void testNodeSingletonRemoteStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteStickyProxy";

        Grid grid = randomGrid();

        // Deploy only on remote nodes.
        grid.services(grid.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        // Get local proxy.
        CounterService svc = grid.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (GridNode n : grid.cluster().forRemotes().nodes()) {
            CounterService rmtSvc =
                    grid.services(grid.cluster().forNode(n)).serviceProxy(name, CounterService.class, false);

            int cnt = rmtSvc.localIncrements();

            assertTrue("Invalid local increments: " + cnt, cnt == 10 || cnt == 0);

            total += rmtSvc.localIncrements();
        }

        assertEquals(10, total);
    }

    /**
     * @throws Exception If failed.
     */
    public void testProxyInvocationFromSeveralNodes() throws Exception {
        final String name = "testProxyInvocationFromSeveralNodes";

        final Grid grid = randomGrid();

        grid.services(grid.cluster().forLocal()).deployNodeSingleton(name, new CacheServiceImpl<String, Integer>());

        int counter = 1;

        grid.services(grid.cluster().forRemotes()).serviceProxy(name, CacheService.class, false).
                put(counter++, "executed");

        assertEquals(nodeCount() - 1, grid.services().serviceProxy(name, CacheService.class, false).size());
    }

    /**
     * Cache service.
     *
     * @param <K> Type of cache keys.
     * @param <V> Type of cache values.
     */
    protected interface CacheService<K, V> {
        void put(K key, V val);

        V get(K key);

        void clear();

        int size();
    }

    /**
     * Cache service implementation.
     */
    protected static class CacheServiceImpl<K, V> implements CacheService<K, V>, GridService {
        /** Underlying cache map. */
        private final Map<K, V> cache = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            cache.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return cache.get(key);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            cache.clear();
        }

        @Override public int size() {
            return cache.size();
        }

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            X.println("Stopping cache service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(GridServiceContext ctx) throws Exception {
            X.println("Initializing counter service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) throws Exception {
            X.println("Executing cache service: " + ctx.name());
        }
    }
}
