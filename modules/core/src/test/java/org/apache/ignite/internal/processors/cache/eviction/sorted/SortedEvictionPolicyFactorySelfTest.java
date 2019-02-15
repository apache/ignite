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

package org.apache.ignite.internal.processors.cache.eviction.sorted;

import javax.cache.configuration.Factory;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicyFactory;
import org.apache.ignite.internal.processors.cache.eviction.EvictionPolicyFactoryAbstractTest;

/**
 * Sorted eviction policy tests.
 */
public class SortedEvictionPolicyFactorySelfTest extends EvictionPolicyFactoryAbstractTest<SortedEvictionPolicy<String, String>> {
    /** {@inheritDoc} */
    @Override protected Factory<SortedEvictionPolicy<String, String>> createPolicyFactory() {
        return new SortedEvictionPolicyFactory<>(plcMax, plcBatchSize, plcMaxMemSize);
    }

    /** {@inheritDoc} */
    @Override protected Factory<SortedEvictionPolicy<String, String>> createNearPolicyFactory(int nearMax) {
        SortedEvictionPolicyFactory<String, String> plc = new SortedEvictionPolicyFactory<>();

        plc.setMaxSize(nearMax);
        plc.setBatchSize(plcBatchSize);

        return plc;
    }

    /** {@inheritDoc} */
    @Override protected void doTestPolicy() throws Exception {
        policyFactory = createPolicyFactory();

        try {
            startGrid();

            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            SortedEvictionPolicy<String, String> p = policy();

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());
            assertFalse(e3.isEvicted());

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e2, e3);

            p.onEntryAccessed(false, e4);

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

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertTrue(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertTrue(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e3, e4, e5);

            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(true, e3);

            check(MockEntry.ENTRY_SIZE, p.queue(), e4, e5);

            assertFalse(e3.isEvicted());
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
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void doTestPolicyWithBatch() throws Exception {
        policyFactory = createPolicyFactory();

        try {
            startGrid();

            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            SortedEvictionPolicy<String, String> p = policy();

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

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e3, e4, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e5);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e3, e4, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
            assertFalse(e5.isEvicted());

            p.onEntryAccessed(false, e1);

            check(MockEntry.ENTRY_SIZE, p.queue(), e1, e3, e4, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());
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

            assertFalse(e3.isEvicted());

            info(p);
        }
        finally {
            stopAllGrids();
        }
    }
}