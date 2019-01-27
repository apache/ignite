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
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test was added to check fix for IGNITE-7883.
 * Detect difference property "keyConfiguration" in cache configuration.
 */
public class CacheAffinityKeyConfigurationMismatchTest extends GridCommonAbstractTest {
    /**
     * Test for matching "keyConfiguration" property
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationMatch() throws Exception {
        try (Ignite ignite0 = getIgnite(0, getCacheKeyConfiguration("a"))) {
            try (Ignite ignite1 = getIgnite(1, getCacheKeyConfiguration("a"))) {
                Affinity<Object> affinity0 = ignite0.affinity(DEFAULT_CACHE_NAME);
                Affinity<Object> affinity1 = ignite1.affinity(DEFAULT_CACHE_NAME);

                for (int i = 0; i < Integer.MAX_VALUE; i = i << 1 | 1) {
                    AKey key = new AKey(i);

                    assertEquals("different affinity partition for key=" + i
                        , affinity0.partition(key)
                        , affinity1.partition(key)
                    );
                }
            }
        }
    }

    /**
     * Test for checking "keyConfiguration" when property has other field name for a specified name type
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationDuplicateTypeName() throws Exception {
        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite = getIgnite(0,
                        getCacheKeyConfiguration("a"),
                        getCacheKeyConfiguration("b")
                    )) {
                    }
                    return null;
                }
            },
            IgniteCheckedException.class, null);
    }

    /**
     * Test when property "keyConfiguration" differs by array size.
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationLengthMismatch() throws Exception {
        GridTestUtils.assertThrowsAnyCause(
            log
            , new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = getIgnite(0, getCacheKeyConfiguration("a"))) {
                        try (Ignite ignite1 = getIgnite(1)) {
                        }
                    }

                    return null;
                }
            }
            , IgniteCheckedException.class
            , "Affinity key configuration mismatch"
        );
    }

    /**
     * Test for checking "keyConfiguration" when property has other field name for a specified name type
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationMismatch() throws Exception {
        GridTestUtils.assertThrowsAnyCause(
            log
            , new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = getIgnite(0, getCacheKeyConfiguration("a"))) {
                        try (Ignite ignite1 = getIgnite(1, getCacheKeyConfiguration("b"))) {
                        }
                    }
                    return null;
                }
            }
            , IgniteCheckedException.class
            , "Affinity key configuration mismatch"
        );
    }

    /**
     * Creating test cache key configuration
     *
     * @param affKeyFieldName Affinity field name.
     * @return Configuration.
     */
    private CacheKeyConfiguration getCacheKeyConfiguration(String affKeyFieldName) {
        return new CacheKeyConfiguration(AKey.class.getCanonicalName(), affKeyFieldName);
    }

    /**
     * Start ignite
     *
     * @param idx For instance name
     * @param cacheKeyCfg Cache key configuration.
     * @return Ignite
     * @throws Exception If failed.
     */
    private Ignite getIgnite(int idx, CacheKeyConfiguration... cacheKeyCfg) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder(true);

        finder.setAddresses(Arrays.asList("localhost:47500..47501"));

        cfg.setIgniteInstanceName("test" + idx);

        CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setKeyConfiguration(cacheKeyCfg);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(finder));

        return startGrid(cfg);
    }

    /**
     * Value structure for test
     */
    private static class AKey {
        /**  */
        int a;

        public AKey(int a) {
            this.a = a;
        }

        @Override public String toString() {
            return "AKey{a=" + a + '}';
        }
    }
}