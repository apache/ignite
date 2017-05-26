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

package org.apache.ignite.cache.affinity.local;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for local affinity function.
 */
public class LocalAffinityFunctionTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 1;

    /** */
    private static final String CACHE1 = "cache1";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

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

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    public void testWronglySetAffinityFunctionForLocalCache() {
        Ignite node = ignite(NODE_CNT - 1);

        CacheConfiguration ccf = node.cache(CACHE1).getConfiguration(CacheConfiguration.class);

        assertEquals("org.apache.ignite.internal.processors.cache.GridCacheProcessor$LocalAffinityFunction",
            ccf.getAffinity().getClass().getName());
    }

}
