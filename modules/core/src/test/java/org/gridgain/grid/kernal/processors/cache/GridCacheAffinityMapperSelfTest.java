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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Test affinity mapper.
 */
public class GridCacheAffinityMapperSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testMethodAffinityMapper() {
        GridCacheAffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

        List<GridCacheAffinityKey<Integer>> keys = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            keys.add(new GridCacheAffinityKey<>(i, Integer.toString(i)));

        for (int i = 1; i <= 10; i++) {
            GridCacheAffinityKey<Integer> key = keys.get(i - 1);

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
        GridCacheAffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

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
        GridCacheAffinityKeyMapper mapper =
            new GridCacheDefaultAffinityKeyMapper();

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
        @GridCacheAffinityKeyMapped
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
