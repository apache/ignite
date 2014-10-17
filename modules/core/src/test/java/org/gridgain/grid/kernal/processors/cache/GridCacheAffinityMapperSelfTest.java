/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.affinity.*;
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
