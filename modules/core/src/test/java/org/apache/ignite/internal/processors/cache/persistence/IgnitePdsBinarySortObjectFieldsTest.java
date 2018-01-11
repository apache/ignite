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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsBinarySortObjectFieldsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "ignitePdsBinarySortObjectFieldsTestCache";

    /**
     * Value.
     */
    public static class Value {
        /** */
        private Long val;

        /**
         * Default constructor.
         */
        public Value() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public Value(Long val) {
            this.val = val;
        }

        /**
         * Returns the value.
         *
         * @return Value.
         */
        public Long getVal() {
            return val;
        }

        /**
         * Sets the value.
         *
         * @param val Value.
         */
        public void setVal(Long val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Value [val=" + val + ']';
        }
    }

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        GridTestUtils.deleteDbFiles();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testGivenCacheWithPojoValueAndPds_WhenPut_ThenNoHangup() throws Exception {
        System.setProperty("IGNITE_BINARY_SORT_OBJECT_FIELDS", "true");

        GridTestUtils.assertTimeout(5, TimeUnit.SECONDS, new Runnable() {
            @Override public void run() {
                IgniteEx ignite;

                try {
                    ignite = startGrid(0);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }

                ignite.active(true);

                IgniteCache<Long, Value> cache = ignite.getOrCreateCache(
                    new CacheConfiguration<Long, Value>(CACHE_NAME)
                );

                cache.put(1L, new Value(1L));

                assertEquals(1, cache.size());
            }
        });
    }
}
