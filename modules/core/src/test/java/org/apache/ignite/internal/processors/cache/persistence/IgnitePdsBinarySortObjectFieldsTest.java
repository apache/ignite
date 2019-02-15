/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
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
