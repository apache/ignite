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
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Basic tests for atomic stamped.
 */
public abstract class GridCacheAtomicStampedApiSelfAbstractTest extends IgniteAtomicsAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrepareAtomicStamped() throws Exception {
        /** Name of first atomic. */
        String atomicName1 = UUID.randomUUID().toString();

        String initVal = "1";
        String initStamp = "2";

        IgniteAtomicStamped<String, String> atomic1 = grid(0).atomicStamped(atomicName1, initVal, initStamp, true);
        IgniteAtomicStamped<String, String> atomic2 = grid(0).atomicStamped(atomicName1, null, null, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);
        assert atomic1.equals(atomic2);
        assert atomic2.equals(atomic1);

        assert initVal.equals(atomic2.value());
        assert initStamp.equals(atomic2.stamp());

        atomic1.close();
        atomic2.close();

        assertNull(grid(0).atomicStamped(atomicName1, null, null, false));

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
    public void testSetAndGet() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";
        String initStamp = "asdfgh";

        IgniteAtomicStamped<String, String> atomic = grid(0).atomicStamped(atomicName, initVal, initStamp, true);

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());
        assertEquals(initVal, atomic.get().get1());
        assertEquals(initStamp, atomic.get().get2());

        atomic.set(null, null);

        assertEquals(null, atomic.value());
        assertEquals(null, atomic.stamp());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompareAndSetSimpleValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";
        String initStamp = "asdfgh";

        IgniteAtomicStamped<String, String> atomic = grid(0).atomicStamped(atomicName, initVal, initStamp, true);

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());
        assertEquals(initVal, atomic.get().get1());
        assertEquals(initStamp, atomic.get().get2());

        atomic.compareAndSet("a", "b", "c", "d");

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());

        atomic.compareAndSet(initVal, null, initStamp, null);

        assertEquals(null, atomic.value());
        assertEquals(null, atomic.stamp());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName("MyCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            String atomicName = UUID.randomUUID().toString();

            String initVal = "qwerty";
            String initStamp = "asdf";

            IgniteAtomicStamped<String, String> atomicStamped = ignite.atomicStamped(atomicName,
                initVal,
                initStamp,
                true);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1,1);

                assertEquals(initVal, atomicStamped.value());
                assertEquals(initStamp, atomicStamped.stamp());
                assertEquals(initVal, atomicStamped.get().get1());
                assertEquals(initStamp, atomicStamped.get().get2());

                assertTrue(atomicStamped.compareAndSet(initVal, "b", initStamp, "d"));

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertEquals("b", atomicStamped.value());
            assertEquals("d", atomicStamped.stamp());

            atomicStamped.close();

            assertTrue(atomicStamped.removed());
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }
}