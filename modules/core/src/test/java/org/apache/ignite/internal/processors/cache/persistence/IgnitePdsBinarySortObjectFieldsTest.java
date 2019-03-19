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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsBinarySortObjectFieldsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "ignitePdsBinarySortObjectFieldsTestCache";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BINARY_SORT_OBJECT_FIELDS, value = "true")
    public void testGivenCacheWithPojoValueAndPds_WhenPut_ThenNoHangup() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        final IgniteCache<Long, Value> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Long, Value>(CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(32)));

        GridTestUtils.assertTimeout(5, TimeUnit.SECONDS, () -> cache.put(1L, new Value(1L)));

        assertEquals(1, cache.size());
    }

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
}
