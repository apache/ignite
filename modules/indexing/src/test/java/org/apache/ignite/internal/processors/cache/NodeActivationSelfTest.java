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
 *
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests functionality related to node activation.
 */
public class NodeActivationSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(null));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected static CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(false);
    }

    /**
     * @throws Exception If fails.
     */
    public void test() throws Exception {
//        final IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("1"));
//
//        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
//            @Override public boolean apply() {
//                return ignite1.context().discovery().activated(ignite1.localNode());
//            }
//        }, 5000);
//
//        assert ignite1.context().discovery().topologyVersionEx().minorTopologyVersion() > 0;
//
//        final IgniteCache cache1 = ignite1.cache(null);
//
//        cache1.put(1, 1);
//
//        final IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("2"));
//
//        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
//            @Override public boolean apply() {
//                return ignite2.context().discovery().activated(ignite2.localNode());
//            }
//        }, 5000);
//
//        assert ignite1.context().discovery().topologyVersionEx().minorTopologyVersion() > 0;
//        assert ignite1.context().discovery().topologyVersionEx().equals(ignite2.context().discovery().topologyVersionEx());
//        assert ignite1.context().discovery().activated(ignite1.localNode());
//        assert ignite1.context().discovery().activated(ignite2.localNode());
//        assert ignite2.context().discovery().activated(ignite1.localNode());
//
//        final IgniteCache cache2 = ignite2.cache(null);
//
//        cache2.put(2, 2);
//
//        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
//            @Override public boolean apply() {
//                return cache2.containsKey(1) && cache1.containsKey(2);
//            }
//        }, 5000);
//
//        assert cache2.get(1).equals(Integer.valueOf(1));
//        assert cache1.get(2).equals(Integer.valueOf(2));
    }
}
