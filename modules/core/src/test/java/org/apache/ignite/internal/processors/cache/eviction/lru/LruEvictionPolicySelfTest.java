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

package org.apache.ignite.internal.processors.cache.eviction.lru;

import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.internal.processors.cache.CacheEvictableEntryImpl;
import org.apache.ignite.internal.processors.cache.eviction.EvictionAbstractTest;

/**
 * LRU Eviction policy tests.
 */
public class LruEvictionPolicySelfTest extends
    EvictionAbstractTest<LruEvictionPolicy<String, String>> {
    /**
     * @throws Exception If failed.
     */
    public void testMiddleAccess() throws Exception {
        startGrid();

        try {
            LruEvictionPolicy<String, String> p = policy();

            int max = 8;

            p.setMaxSize(max * MockEntry.ENTRY_SIZE);

            MockEntry entry1 = new MockEntry("1", "1");
            MockEntry entry2 = new MockEntry("2", "2");
            MockEntry entry3 = new MockEntry("3", "3");

            p.onEntryAccessed(false, entry1);
            p.onEntryAccessed(false, entry2);
            p.onEntryAccessed(false, entry3);

            MockEntry[] freqUsed = new MockEntry[] {
                new MockEntry("4", "4"),
                new MockEntry("5", "5"),
                new MockEntry("6", "6"),
                new MockEntry("7", "7"),
                new MockEntry("8", "7")
            };

            for (MockEntry e : freqUsed)
                p.onEntryAccessed(false, e);

            for (MockEntry e : freqUsed)
                assert !e.isEvicted();

            int cnt = 1001;

            for (int i = 0; i < cnt; i++)
                p.onEntryAccessed(false, entry(freqUsed, i % freqUsed.length));

            info(p);

            check(max, MockEntry.ENTRY_SIZE);
        }
        finally {
            stopGrid();
        }
    }

    /** {@inheritDoc} */
    @Override protected void doTestPolicy() throws Exception {
        startGrid();

        try {
            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            LruEvictionPolicy<String, String> p = policy();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());

            p.onEntryAccessed(false, e4);

            check(p.queue(), e2, e3, e4);
            check(MockEntry.ENTRY_SIZE, p.queue(), e2, e3, e4);

            assertTrue(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertTrue(e2.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1 = new MockEntry("1", "1"));

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assertTrue(e3.isEvicted());
            assertFalse(e1.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e5);

            assertEquals(3, p.getCurrentSize());

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e1, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assertFalse(e1.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e5);

            assertEquals(3, p.getCurrentSize());

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e1, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e5);

            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e5);

            check(MockEntry.ENTRY_SIZE, p.queue());

            assertFalse(e5.isEvicted());

            info(p);
        }
        finally {
            stopGrid();
        }
    }

    /** {@inheritDoc} */
    @Override protected void doTestPolicyWithBatch() throws Exception {
        startGrid();

        try {
            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            LruEvictionPolicy<String, String> p = policy();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());

            p.onEntryAccessed(false, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3, e4);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());

            p.onEntryAccessed(false, e5);

            // Batch evicted
            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertTrue(e1.isEvicted());
            assertTrue(e2.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1 = new MockEntry("1", "1"));

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5, e1);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());
            assertFalse(e1.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e1, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e1.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5, e1);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());
            assertFalse(e1.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e1, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e1.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3);

            assertFalse(e3.isEvicted());

            p.onEntryAccessed(true, e3);

            check(MockEntry.ENTRY_SIZE, p.queue());

            info(p);
        }
        finally {
            stopGrid();
        }
    }

    /** {@inheritDoc} */
    @Override protected LruEvictionPolicy<String, String> createPolicy(int plcMax) {
        LruEvictionPolicy<String, String> plc = new LruEvictionPolicy<>();

        plc.setMaxSize(this.plcMax);
        plc.setBatchSize(this.plcBatchSize);
        plc.setMaxMemorySize(this.plcMaxMemSize);

        return plc;
    }

    /** {@inheritDoc} */
    @Override protected LruEvictionPolicy<String, String> createNearPolicy(int nearMax) {
        LruEvictionPolicy<String, String> plc = new LruEvictionPolicy<>();

        plc.setMaxSize(nearMax);
        plc.setBatchSize(plcBatchSize);

        return plc;
    }

    /** {@inheritDoc} */
    @Override protected void checkNearPolicies(int endNearPlcSize) {
        for (int i = 0; i < gridCnt; i++)
            for (EvictableEntry<String, String> e : nearPolicy(i).queue())
                assert !e.isCached() : "Invalid near policy size: " + nearPolicy(i).queue();
    }

    /** {@inheritDoc} */
    @Override protected void checkPolicies() {
        for (int i = 0; i < gridCnt; i++) {
            if (plcMaxMemSize > 0) {
                int size = 0;

                for (EvictableEntry<String, String> entry : policy(i).queue())
                    size += ((CacheEvictableEntryImpl)entry).size();

                assertEquals(size, ((LruEvictionPolicy)policy(i)).getCurrentMemorySize());
            }
            else
                assertTrue(policy(i).queue().size() <= plcMax + plcBatchSize);
        }
    }

}