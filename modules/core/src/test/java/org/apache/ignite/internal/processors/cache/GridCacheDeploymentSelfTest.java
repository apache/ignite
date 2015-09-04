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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Cache + Deployment test.
 */
public class GridCacheDeploymentSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name for grid without cache. */
    private static final String GRID_NAME = "grid-no-cache";

    /** First test task name. */
    private static final String TEST_TASK_1 = "org.apache.ignite.tests.p2p.CacheDeploymentTestTask1";

    /** Second test task name. */
    private static final String TEST_TASK_2 = "org.apache.ignite.tests.p2p.CacheDeploymentTestTask2";

    /** Third test task name. */
    private static final String TEST_TASK_3 = "org.apache.ignite.tests.p2p.CacheDeploymentTestTask3";

    /** Test value 1. */
    private static final String TEST_KEY = "org.apache.ignite.tests.p2p.CacheDeploymentTestKey";

    /** Test value 1. */
    private static final String TEST_VALUE_1 = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** Test value 2. */
    private static final String TEST_VALUE_2 = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue2";

    /** */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        if (GRID_NAME.equals(gridName))
            cfg.setCacheConfiguration();
        else
            cfg.setCacheConfiguration(cacheConfiguration());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setRebalanceMode(SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setBackups(1);

        return cfg;
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment() throws Exception {
        try {
            depMode = CONTINUOUS;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            Ignite g0 = startGrid(GRID_NAME);

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_1);

            g0.compute().execute(cls, g1.cluster().localNode());

            cls = ldr.loadClass(TEST_TASK_2);

            g0.compute().execute(cls, g2.cluster().localNode());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment2() throws Exception {
        try {
            depMode = CONTINUOUS;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            Ignite g0 = startGrid(GRID_NAME);

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_3);

            String key = "";

            for (int i = 0; i < 1000; i++) {
                key = "1" + i;

                if (g1.cluster().mapKeyToNode(null, key).id().equals(g2.cluster().localNode().id()))
                    break;
            }

            g0.compute().execute(cls, new T2<>(g1.cluster().localNode(), key));

            cls = ldr.loadClass(TEST_TASK_2);

            g0.compute().execute(cls, g2.cluster().localNode());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment3() throws Exception {
        try {
            depMode = SHARED;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            Ignite g0 = startGrid(GRID_NAME);

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_3);

            String key = "";

            for (int i = 0; i < 1000; i++) {
                key = "1" + i;

                if (g1.cluster().mapKeyToNode(null, key).id().equals(g2.cluster().localNode().id()))
                    break;
            }

            g0.compute().execute(cls, new T2<>(g1.cluster().localNode(), key));

            stopGrid(GRID_NAME);

            for (int i = 0; i < 10; i++) {
                if (g1.cache(null).localSize() == 0 && g2.cache(null).localSize() == 0)
                    break;

                U.sleep(500);
            }

            assertEquals(0, g1.cache(null).localSize());
            assertEquals(0, g2.cache(null).localSize());

            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment4() throws Exception {
        try {
            depMode = CONTINUOUS;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            Ignite g0 = startGrid(GRID_NAME);

            info("Started grids:");
            info("g0: " + g0.cluster().localNode().id());
            info("g1: " + g1.cluster().localNode().id());
            info("g2: " + g2.cluster().localNode().id());

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_3);

            String key = "";

            for (int i = 0; i < 1000; i++) {
                key = "1" + i;

                if (g1.cluster().mapKeyToNode(null, key).id().equals(g2.cluster().localNode().id()))
                    break;
            }

            g0.compute().execute(cls, new T2<>(g1.cluster().localNode(), key));

            stopGrid(GRID_NAME);

            assert g1.cache(null).localSize(CachePeekMode.ALL) == 1;
            assert g1.cache(null).localSize(CachePeekMode.ALL) == 1;

            assert g2.cache(null).localSize(CachePeekMode.ALL) == 1;
            assert g2.cache(null).localSize(CachePeekMode.ALL) == 1;

            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment5() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class val1Cls = ldr.loadClass(TEST_VALUE_1);
        Class val2Cls = ldr.loadClass(TEST_VALUE_2);
        Class task2Cls = ldr.loadClass(TEST_TASK_2);

        try {
            depMode = SHARED;

            Ignite g0 = startGrid(0);
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            info(">>>>>>> Grid 0: " + g0.cluster().localNode().id());
            info(">>>>>>> Grid 1: " + g1.cluster().localNode().id());
            info(">>>>>>> Grid 2: " + g2.cluster().localNode().id());

            int key = 0;

            key = getNextKey(key, g0, g1.cluster().localNode(), g2.cluster().localNode(), g0.cluster().localNode());

            info("Key: " + key);

            IgniteCache<Object, Object> cache = g0.cache(null);

            assert cache != null;

            cache.put(key, Arrays.asList(val1Cls.newInstance()));

            info(">>>>>>> First put completed.");

            key = getNextKey(key + 1, g0, g2.cluster().localNode(), g0.cluster().localNode(), g1.cluster().localNode());

            info("Key: " + key);

            cache.put(key, val1Cls.newInstance());

            info(">>>>>>> Second put completed.");

            key = getNextKey(key + 1, g0, g1.cluster().localNode(), g2.cluster().localNode(), g0.cluster().localNode());

            info("Key: " + key);

            cache.put(key, val2Cls.newInstance());

            info(">>>>>>> Third put completed.");

            g0.compute().execute(task2Cls, g1.cluster().localNode());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment6() throws Exception {
        try {
            depMode = SHARED;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_3);

            String key = "";

            for (int i = 0; i < 1000; i++) {
                key = "1" + i;

                if (g1.cluster().mapKeyToNode(null, key).id().equals(g2.cluster().localNode().id()))
                    break;
            }

            g1.compute().execute(cls, new T2<>(g2.cluster().localNode(), key));

            stopGrid(1);

            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testDeployment7() throws Exception {
        try {
            depMode = SHARED;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            ClassLoader ldr = getExternalClassLoader();

            Class cls = ldr.loadClass(TEST_TASK_3);

            String key = "";

            for (int i = 0; i < 1000; i++) {
                key = "1" + i;

                if (g1.cluster().mapKeyToNode(null, key).id().equals(g2.cluster().localNode().id()))
                    break;
            }

            g2.compute().execute(cls, new T2<>(g2.cluster().localNode(), key));

            stopGrid(2);

            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPartitionedDeploymentPreloading() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class valCls = ldr.loadClass(TEST_VALUE_1);

        try {
            depMode = SHARED;

            Ignite g = startGrid(0);

            g.cache(null).put(0, valCls.newInstance());

            info("Added value to cache 0.");

            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Looks for next key starting from {@code start} for which primary node is {@code primary} and backup is {@code
     * backup}.
     *
     * @param start Key to start search from, inclusive.
     * @param g Grid on which check will be performed.
     * @param primary Expected primary node.
     * @param backup Expected backup node.
     * @param near Expected near node.
     * @return Key with described properties.
     * @throws IllegalStateException if such a key could not be found after 10000 iterations.
     */
    private int getNextKey(int start, Ignite g, ClusterNode primary, ClusterNode backup, ClusterNode near) {
        info("Primary: " + primary);
        info("Backup: " + backup);
        info("Near: " + near);

        for (int i = start; i < start + 10000; i++) {
            if (g.affinity(null).isPrimary(primary, i) && g.affinity(null).isBackup(backup, i)) {
                assert !g.affinity(null).isPrimary(near, i) : "Key: " + i;
                assert !g.affinity(null).isBackup(near, i) : "Key: " + i;

                return i;
            }
        }

        throw new IllegalStateException("Unable to find matching key [start=" + start + ", primary=" + primary.id() +
            ", backup=" + backup.id() + ']');
    }
}