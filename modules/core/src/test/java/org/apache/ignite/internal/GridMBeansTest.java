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

package org.apache.ignite.internal;

import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.management.ObjectName;

/**
 * Tests for the standard JMX beans registered by the kernal.
 */
public class GridMBeansTest extends GridCommonAbstractTest {
    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_0 = "Custom executor 0";

    /** Executor name for setExecutorConfiguration */
    private static final String CUSTOM_EXECUTOR_1 = "Custom executor 1";

    /** Create test and auto-start the grid */
    public GridMBeansTest() {
        super(true);
    }

    /**
     * {@inheritDoc}
     *
     * This implementation registers adds custom executors to the configuration.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setExecutorConfiguration(new ExecutorConfiguration(CUSTOM_EXECUTOR_0),
            new ExecutorConfiguration(CUSTOM_EXECUTOR_1));

        FifoEvictionPolicyFactory<String, String> plc = new FifoEvictionPolicyFactory<>();
        plc.setMaxSize(100);
        plc.setBatchSize(10);
        plc.setMaxMemorySize(20);

        CacheConfiguration cache1 = defaultCacheConfiguration();
        cache1.setName("cache1");
        cache1.setOnheapCacheEnabled(true);
        cache1.setEvictionPolicyFactory(plc);

        NearCacheConfiguration ncf = new NearCacheConfiguration<>();
        LruEvictionPolicy lep = new LruEvictionPolicy();
        lep.setBatchSize(10);
        lep.setMaxMemorySize(500);
        lep.setMaxSize(40);
        ncf.setNearEvictionPolicy(lep);
        cache1.setNearConfiguration(ncf);



        CacheConfiguration cache2 = defaultCacheConfiguration();
        lep = new LruEvictionPolicy();
        lep.setBatchSize(10);
        lep.setMaxMemorySize(125);
        lep.setMaxSize(30);

        cache2.setEvictionPolicy(lep);
        cache2.setOnheapCacheEnabled(true);
        cache2.setName("cache2");




        cfg.setCacheConfiguration(cache1,cache2);


        return cfg;
    }

    /** Check that kernal bean is available */
    public void testKernalBeans() throws Exception {
        checkBean("Kernal", "IgniteKernal", "InstanceName", grid().name());
        checkBean("Kernal", "ClusterMetricsMXBeanImpl", "TotalServerNodes", 1);
        checkBean("Kernal", "ClusterMetricsMXBeanImpl", "TotalServerNodes", 1);
    }

    /** Check that eviction bean is available */
    public void testEvictionPolicyBeans() throws Exception{
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "MaxSize", 100);
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "BatchSize", 10);
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "MaxMemorySize", 20L);

        checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxSize", 40);
        checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "BatchSize", 10);
        checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxMemorySize", 500L);

        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxSize", 30);
        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "BatchSize", 10);
        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxMemorySize", 125L);
    }


    /** Check that kernal bean is available */
    public void testExecutorBeans() throws Exception {
        // standard executors
        checkBean("Thread Pools", "GridExecutionExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridSystemExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridManagementExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridClassLoadingExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridQueryExecutor", "Terminated", false);
        checkBean("Thread Pools", "GridSchemaExecutor", "Terminated", false);
        checkBean("Thread Pools", "StripedExecutor", "Terminated", false);

        // custom executors
        checkBean("Thread Pools", CUSTOM_EXECUTOR_0, "Terminated", false);
        checkBean("Thread Pools", CUSTOM_EXECUTOR_1, "Terminated", false);
    }

    /** Checks that a bean with the specified group and name is available and has the expected attribute */
    private void checkBean(String grp, String name, String attributeName, Object expAttributeVal) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(grid().name(), grp, name);
        Object attributeVal = grid().configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        assertEquals(expAttributeVal, attributeVal);
    }
}
