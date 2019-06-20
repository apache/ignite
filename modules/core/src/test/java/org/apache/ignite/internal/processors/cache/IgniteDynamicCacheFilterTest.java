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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteDynamicCacheFilterTest extends GridCommonAbstractTest {
    /** */
    private static final String ATTR_NAME = "cacheAttr";

    /** */
    private String attrVal;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setRebalanceMode(SYNC);

        ccfg.setNodeFilter(new TestNodeFilter("A"));
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (attrVal != null) {
            Map<String, Object> attrs = new HashMap<>();

            attrs.put(ATTR_NAME, attrVal);

            cfg.setUserAttributes(attrs);
        }

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCofiguredCacheFilter() throws Exception {
        attrVal = "A";

        Ignite ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache0);

        cache0.put(1, 1);

        attrVal = null;

        Ignite ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache1);

        attrVal = "B";

        Ignite ignite2 = startGrid(2);

        IgniteCache<Integer, Integer> cache2 = ignite2.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache2);

        attrVal = "A";

        Ignite ignite3 = startGrid(3);

        IgniteCache<Integer, Integer> cache3 = ignite3.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache3);

        assertNotNull(cache0.localPeek(1));
        assertNotNull(cache3.localPeek(1));

        assertNull(cache1.localPeek(1));
        assertNull(cache2.localPeek(1));
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String val;

        /**
         * @param val Attribute value.
         */
        public TestNodeFilter(String val) {
            assert val != null;

            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return val.equals(node.attribute(ATTR_NAME));
        }
    }
}
