/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.ignite.internal.processors.cache;

import javax.management.ObjectName;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for the eviction policy JMX beans registered by the kernal.
 */
public class GridEvictionPolicyMBeansTest extends GridCommonAbstractTest {
    /** Create test and auto-start the grid */
    public GridEvictionPolicyMBeansTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        super.beforeTestsStarted();
    }

    /**
     * {@inheritDoc}
     *
     * This implementation  adds eviction policies.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FifoEvictionPolicyFactory<String, String> plc = new FifoEvictionPolicyFactory<>();

        plc.setMaxSize(100);
        plc.setBatchSize(10);
        plc.setMaxMemorySize(20);

        CacheConfiguration cache1 = defaultCacheConfiguration();

        cache1.setName("cache1");
        cache1.setOnheapCacheEnabled(true);
        cache1.setEvictionPolicyFactory(plc);

        NearCacheConfiguration ncf = new NearCacheConfiguration<>();

        ncf.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(40, 10, 500));

        CacheConfiguration cache2 = defaultCacheConfiguration();

        cache2.setName("cache2");
        cache2.setOnheapCacheEnabled(true);

        LruEvictionPolicy lep = new LruEvictionPolicy();

        lep.setBatchSize(10);
        lep.setMaxMemorySize(125);
        lep.setMaxSize(30);
        cache2.setEvictionPolicy(lep);

        ncf = new NearCacheConfiguration<>();
        lep = new LruEvictionPolicy();

        lep.setBatchSize(10);
        lep.setMaxMemorySize(500);
        lep.setMaxSize(40);
        ncf.setNearEvictionPolicy(lep);

        if (!MvccFeatureChecker.forcedMvcc() || MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE)) {
            cache1.setNearConfiguration(ncf);
            cache2.setNearConfiguration(ncf);
        }

        cfg.setCacheConfiguration(cache1, cache2);

        return cfg;
    }

    /** Check that eviction bean is available */
    @Test
    public void testEvictionPolicyBeans() throws Exception {
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "MaxSize", 100);
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "BatchSize", 10);
        checkBean("cache1", "org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy", "MaxMemorySize", 20L);

        if (!MvccFeatureChecker.forcedMvcc() || MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE)) {
            checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxSize", 40);
            checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "BatchSize", 10);
            checkBean("cache1-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxMemorySize", 500L);
        }

        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxSize", 30);
        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "BatchSize", 10);
        checkBean("cache2", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxMemorySize", 125L);

        if (!MvccFeatureChecker.forcedMvcc() || MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE)) {
            checkBean("cache2-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxSize", 40);
            checkBean("cache2-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "BatchSize", 10);
            checkBean("cache2-near", "org.apache.ignite.cache.eviction.lru.LruEvictionPolicy", "MaxMemorySize", 500L);
        }
    }

    /** Checks that a bean with the specified group and name is available and has the expected attribute */
    private void checkBean(String grp, String name, String attributeName, Object expAttributeVal) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(grid().name(), grp, name);
        Object attributeVal = grid().configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        assertEquals(expAttributeVal, attributeVal);
    }
}
