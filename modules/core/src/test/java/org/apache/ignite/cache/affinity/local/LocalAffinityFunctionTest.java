/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.affinity.local;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for local affinity function.
 */
@RunWith(JUnit4.class)
public class LocalAffinityFunctionTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_CNT = 1;

    /** */
    private static final String CACHE1 = "cache1";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(1);
        ccfg.setName(CACHE1);
        ccfg.setCacheMode(CacheMode.LOCAL);
        ccfg.setAffinity(new RendezvousAffinityFunction());
        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        startGrids(NODE_CNT);
    }

    @Test
    public void testWronglySetAffinityFunctionForLocalCache() {
        Ignite node = ignite(NODE_CNT - 1);

        CacheConfiguration ccf = node.cache(CACHE1).getConfiguration(CacheConfiguration.class);

        assertEquals("org.apache.ignite.internal.processors.cache.GridCacheProcessor$LocalAffinityFunction",
            ccf.getAffinity().getClass().getName());
    }

}
