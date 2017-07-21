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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test affinity mapper.
 */
public class GridCacheAffinityMapperSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testMethodAffinityMapper() {
        AffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

        GridTestUtils.setFieldValue(mapper, "ignite", grid());

        List<AffinityKey<Integer>> keys = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            keys.add(new AffinityKey<>(i, Integer.toString(i)));

        for (int i = 1; i <= 10; i++) {
            AffinityKey<Integer> key = keys.get(i - 1);

            Object mapped = mapper.affinityKey(key);

            info("Mapped key: " + mapped);

            assertNotNull(mapped);
            assertSame(key.affinityKey(), mapped);
        }
    }

    /**
     *
     */
    public void testFieldAffinityMapper() {
        AffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

        GridTestUtils.setFieldValue(mapper, "ignite", grid());

        List<FieldAffinityKey<Integer>> keys = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            keys.add(new FieldAffinityKey<>(i, Integer.toString(i)));

        for (int i = 1; i <= 10; i++) {
            FieldAffinityKey<Integer> key = keys.get(i - 1);

            Object mapped = mapper.affinityKey(key);

            info("Mapped key: " + mapped);

            assertNotNull(mapped);
            assertSame(key.affinityKey(), mapped);
        }
    }

    /**
     *
     */
    public void testFieldAffinityMapperWithWrongClass() {
        AffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

        GridTestUtils.setFieldValue(mapper, "ignite", grid());

        FieldNoAffinityKey key = new FieldNoAffinityKey();
        Object mapped = mapper.affinityKey(key);
        assertEquals(key, mapped);
    }

    /**
     * Test key for field annotation.
     */
    private static class FieldNoAffinityKey {
        // No-op.
    }

    /**
     * Test key for field annotation.
     */
    private static class FieldAffinityKey<K> {
        /** Key. */
        private K key;

        /** Affinity key. */
        @AffinityKeyMapped
        private Object affKey;

        /**
         * Initializes key together with its affinity key counter-part.
         *
         * @param key Key.
         * @param affKey Affinity key.
         */
        FieldAffinityKey(K key, Object affKey) {
            this.key = key;
            this.affKey = affKey;
        }

        /**
         * @return Key.
         */
        public K key() {
            return key;
        }

        /**
         * @return Affinity key.
         */
        public Object affinityKey() {
            return affKey;
        }
    }
}