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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Basic tests for atomic reference.
 */
public abstract class GridCacheAtomicReferenceApiSelfAbstractTest extends IgniteAtomicsAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareAtomicReference() throws Exception {
        /* Name of first atomic. */
        String atomicName1 = UUID.randomUUID().toString();

        /* Name of second atomic. */
        String atomicName2 = UUID.randomUUID().toString();

        String initVal = "1";
        IgniteAtomicReference<String> atomic1 = grid(0).atomicReference(atomicName1, initVal, true);
        IgniteAtomicReference<String> atomic2 = grid(0).atomicReference(atomicName2, null, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);

        atomic1.close();
        atomic2.close();

        atomic1.close();
        atomic2.close();

        assertNull(grid(0).atomicReference(atomicName1, null, false));
        assertNull(grid(0).atomicReference(atomicName2, null, false));

        try {
            atomic1.get();

            fail();
        }
        catch (IllegalStateException | IgniteException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetAndGet() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";

        IgniteAtomicReference<String> atomic = grid(0).atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.set(null);

        assertEquals(null, atomic.get());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCompareAndSetSimpleValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";

        IgniteAtomicReference<String> atomic = grid(0).atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet("h", "j");

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet(initVal, null);

        assertEquals(null, atomic.get());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCompareAndSetNullValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        IgniteAtomicReference<String> atomic = grid(0).atomicReference(atomicName, null, true);

        assertEquals(null, atomic.get());

        boolean success = atomic.compareAndSet(null, "newVal");

        assertTrue(success);
        assertEquals("newVal", atomic.get());
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure
     * that transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            String atomicName = UUID.randomUUID().toString();

            String initValue = "qazwsx";

            IgniteAtomicReference<String> atomicReference = ignite.atomicReference(atomicName, initValue, true);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                assertEquals(initValue, atomicReference.get());

                assertTrue(atomicReference.compareAndSet(initValue, "aaa"));

                assertEquals("aaa", atomicReference.get());

                tx.rollback();

                assertEquals(0, cache.size());
            }

            assertTrue(atomicReference.compareAndSet("aaa", null));

            assertNull(atomicReference.get());

            atomicReference.close();

            assertTrue(atomicReference.removed());
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }

    /**
     * Tests that basic API works correctly when there are multiple structures in multiple groups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStructuresInDifferentGroups() throws Exception {
        Ignite ignite = grid(0);

        IgniteAtomicReference<String> ref1 = ignite.atomicReference("ref1", "a", true);
        IgniteAtomicReference<String> ref2 = ignite.atomicReference("ref2", "b", true);
        IgniteAtomicReference<String> ref3 = ignite.atomicReference("ref3", "c", true);

        AtomicConfiguration cfg = new AtomicConfiguration().setGroupName("grp1");

        IgniteAtomicReference<String> ref4 = ignite.atomicReference("ref4", cfg, "d", true);
        IgniteAtomicReference<String> ref5 = ignite.atomicReference("ref5", cfg, "e", true);
        IgniteAtomicReference<String> ref6 = ignite.atomicReference("ref6", cfg, "f", true);

        assertNull(ignite.atomicReference("ref4", "a", false));
        assertNull(ignite.atomicReference("ref5", "a", false));
        assertNull(ignite.atomicReference("ref6", "a", false));

        assertNull(ignite.atomicReference("ref1", cfg, "a", false));
        assertNull(ignite.atomicReference("ref2", cfg, "a", false));
        assertNull(ignite.atomicReference("ref3", cfg, "a", false));

        assertTrue(ref1.compareAndSet("a", "A"));
        assertTrue(ref2.compareAndSet("b", "B"));
        assertTrue(ref3.compareAndSet("c", "C"));
        assertTrue(ref4.compareAndSet("d", "D"));
        assertTrue(ref5.compareAndSet("e", "E"));
        assertTrue(ref6.compareAndSet("f", "F"));

        assertFalse(ref1.compareAndSet("a", "Z"));
        assertFalse(ref2.compareAndSet("b", "Z"));
        assertFalse(ref3.compareAndSet("c", "Z"));
        assertFalse(ref4.compareAndSet("d", "Z"));
        assertFalse(ref5.compareAndSet("e", "Z"));
        assertFalse(ref6.compareAndSet("f", "Z"));

        assertEquals("A", ref1.get());
        assertEquals("B", ref2.get());
        assertEquals("C", ref3.get());
        assertEquals("D", ref4.get());
        assertEquals("E", ref5.get());
        assertEquals("F", ref6.get());

        ref2.close();
        ref5.close();

        assertTrue(ref2.removed());
        assertTrue(ref5.removed());

        assertNull(ignite.atomicReference("ref2", "b", false));
        assertNull(ignite.atomicReference("ref5", cfg, "e", false));

        assertFalse(ref1.removed());
        assertFalse(ref3.removed());
        assertFalse(ref4.removed());
        assertFalse(ref6.removed());

        assertNotNull(ignite.atomicReference("ref1", "a", false));
        assertNotNull(ignite.atomicReference("ref3", "c", false));
        assertNotNull(ignite.atomicReference("ref4", cfg, "d", false));
        assertNotNull(ignite.atomicReference("ref6", cfg, "f", false));
    }
}
