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

package org.apache.ignite.internal.processors.cache.eviction.fifo;

import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.internal.processors.cache.eviction.EvictionAbstractTest;

/**
 * FIFO eviction policy tests.
 */
public class FifoEvictionPolicySelfTest extends
    EvictionAbstractTest<FifoEvictionPolicy<String, String>> {
    /** {@inheritDoc} */
    @Override protected void doTestPolicy() throws Exception {
        try {
            startGrid();

            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            FifoEvictionPolicy<String, String> p = policy();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            assert !e1.isEvicted();
            assert !e2.isEvicted();
            assert !e3.isEvicted();

            p.onEntryAccessed(false, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e2, e3, e4);

            assert e1.isEvicted();
            assert !e2.isEvicted();
            assert !e3.isEvicted();
            assert !e4.isEvicted();

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assert e2.isEvicted();
            assert !e3.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e1 = new MockEntry("1", "1"));

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assert e3.isEvicted();
            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5, e1);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e5);

            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e5);

            check(MockEntry.ENTRY_SIZE, p.queue());

            assert !e5.isEvicted();

            info(p);
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void doTestPolicyWithBatch() throws Exception {
        try {
            startGrid();

            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            FifoEvictionPolicy<String, String> p = policy();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            p.onEntryAccessed(false, e4);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3, e4);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());

            p.onEntryAccessed(false, e5);

            // Batch evicted.
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

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5, e1);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());
            assertFalse(e1.isEvicted());

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5, e1);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());
            assertFalse(e1.isEvicted());

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

            assertFalse(e3.isEvicted());

            info(p);
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected FifoEvictionPolicy<String, String> createPolicy(int plcMax) {
        FifoEvictionPolicy<String, String> plc = new FifoEvictionPolicy<>();

        plc.setMaxSize(this.plcMax);
        plc.setBatchSize(this.plcBatchSize);
        plc.setMaxMemorySize(this.plcMaxMemSize);

        return plc;
    }

    /** {@inheritDoc} */
    @Override protected FifoEvictionPolicy<String, String> createNearPolicy(int nearMax) {
        FifoEvictionPolicy<String, String> plc = new FifoEvictionPolicy<>();

        plc.setMaxSize(nearMax);
        plc.setBatchSize(plcBatchSize);

        return plc;
    }
}