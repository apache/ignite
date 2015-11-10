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

import java.util.HashSet;
import java.util.Map;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Cache EntryProcessor + Deployment.
 */
public class GridCacheAtomicEntryProcessorDeploymentSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Entry processor */
    protected static String TEST_ENT_PROCESSOR = "org.apache.ignite.tests.p2p.CacheDeploymentEntryProcessor";

    /** Test value. */
    protected static String TEST_VALUE = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** */
    private DeploymentMode depMode;

    /** */
    private boolean cliendMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cliendMode)
            cfg.setClientMode(cliendMode);

        cfg.setDeploymentMode(depMode);

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
        cfg.setAtomicityMode(atomicityMode());
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setBackups(1);

        return cfg;
    }

    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvokeDeployment() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestInvoke();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvokeDeployment2() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestInvoke();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvokeAllDeployment() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestInvokeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvokeAllDeployment2() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestInvokeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    private void doTestInvoke() throws Exception {
        try {
            cliendMode = false;
            startGrid(0);

            cliendMode = true;
            startGrid(1);

            ClassLoader ldr = getExternalClassLoader();

            Class procCls = ldr.loadClass(TEST_ENT_PROCESSOR);
            Class valCls = ldr.loadClass(TEST_VALUE);

            assertTrue(grid(1).configuration().isClientMode());

            IgniteCache cache = grid(1).cache(null);

            cache.put("key", valCls.newInstance());

            Boolean res = (Boolean)cache.invoke("key", (CacheEntryProcessor)procCls.newInstance());

            assertTrue(res);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    private void doTestInvokeAll() throws Exception {
        try {
            cliendMode = false;
            startGrid(0);

            cliendMode = true;
            startGrid(1);

            ClassLoader ldr = getExternalClassLoader();

            Class procCls = ldr.loadClass(TEST_ENT_PROCESSOR);
            Class valCls = ldr.loadClass(TEST_VALUE);

            assertTrue(grid(1).configuration().isClientMode());

            IgniteCache cache = grid(1).cache(null);

            HashSet keys = new HashSet();

            for (int i = 0; i < 3; i++) {
                String key = "key" + i;

                cache.put(key, valCls.newInstance());

                keys.add(key);
            }

            Map<String, EntryProcessorResult> res = (Map<String, EntryProcessorResult>)cache.invokeAll(keys,
                (CacheEntryProcessor)procCls.newInstance());

            assertEquals(3, res.size());

            for (EntryProcessorResult result : res.values())
                assertTrue((Boolean)result.get());
        }
        finally {
            stopAllGrids();
        }
    }
}
