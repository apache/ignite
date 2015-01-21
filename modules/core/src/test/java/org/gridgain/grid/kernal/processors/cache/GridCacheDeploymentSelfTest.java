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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.IgniteTxIsolation.REPEATABLE_READ;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache + Deployment test.
 */
public class GridCacheDeploymentSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name for grid without cache. */
    private static final String GRID_NAME = "grid-no-cache";

    /** First test task name. */
    private static final String TEST_TASK_1 = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestTask1";

    /** Second test task name. */
    private static final String TEST_TASK_2 = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestTask2";

    /** Third test task name. */
    private static final String TEST_TASK_3 = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestTask3";

    /** Test value 1. */
    private static final String TEST_KEY = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestKey";

    /** Test value 1. */
    private static final String TEST_VALUE_1 = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestValue";

    /** Test value 2. */
    private static final String TEST_VALUE_2 = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestValue2";

    /** */
    private IgniteDeploymentMode depMode;

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

        cfg.setRestEnabled(false);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected GridCacheConfiguration cacheConfiguration() throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setPreloadMode(SYNC);
        cfg.setStoreValueBytes(true);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setDistributionMode(NEAR_PARTITIONED);
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
                if (g1.cache(null).isEmpty() && g2.cache(null).isEmpty())
                    break;

                U.sleep(500);
            }

            assert g1.cache(null).isEmpty();
            assert g2.cache(null).isEmpty();

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

            assert g1.cache(null).size() == 1;
            assert g1.cache(null).size() == 1;

            assert g2.cache(null).size() == 1;
            assert g2.cache(null).size() == 1;

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

            GridCache<Object, Object> cache = g0.cache(null);

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

    /** @throws Exception If failed. */
    public void _testDeploymentGroupLock() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class<?> keyCls = ldr.loadClass(TEST_KEY);

        try {
            depMode = SHARED;

            Ignite g1 = startGrid(1);
            startGrid(2);

            Constructor<?> constructor = keyCls.getDeclaredConstructor(String.class);

            Object affKey;

            int i = 0;

            do {
                affKey = constructor.newInstance(String.valueOf(i));

                i++;
            }
            while (!g1.cluster().mapKeyToNode(null, affKey).id().equals(g1.cluster().localNode().id()));

            GridCache<Object, Object> cache = g1.cache(null);

            try (IgniteTx tx = cache.txStartAffinity(affKey, PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                cache.put(new GridCacheAffinityKey<>("key1", affKey), "val1");

                tx.commit();
            }

            assertEquals("val1", cache.get(new GridCacheAffinityKey<>("key1", affKey)));
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
            if (g.cache(null).affinity().isPrimary(primary, i) && g.cache(null).affinity().isBackup(backup, i)) {
                assert !g.cache(null).affinity().isPrimary(near, i) : "Key: " + i;
                assert !g.cache(null).affinity().isBackup(near, i) : "Key: " + i;

                return i;
            }
        }

        throw new IllegalStateException("Unable to find matching key [start=" + start + ", primary=" + primary.id() +
            ", backup=" + backup.id() + ']');
    }
}
