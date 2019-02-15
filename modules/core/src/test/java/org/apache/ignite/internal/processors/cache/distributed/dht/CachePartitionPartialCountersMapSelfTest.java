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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class CachePartitionPartialCountersMapSelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testAddAndRemove() throws Exception {
        CachePartitionPartialCountersMap map = new CachePartitionPartialCountersMap(10);

        for (int p = 0; p < 10; p++)
            map.add(p, 2 * p, 3 * p);

        for (int p = 0; p < 10; p++) {
            assertEquals(p, map.partitionAt(p));
            assertEquals(2 * p, map.initialUpdateCounterAt(p));
            assertEquals(3 * p, map.updateCounterAt(p));
        }

        map.remove(3);
        map.remove(11);
        map.remove(7);

        assertEquals(8, map.size());

        int idx = 0;

        for (int p = 0; p < 10; p++) {
            if (p == 3 || p == 10 || p == 7)
                continue;

            assertEquals(p, map.partitionAt(idx));
            assertEquals(2 * p, map.initialUpdateCounterAt(idx));
            assertEquals(3 * p, map.updateCounterAt(idx));

            idx++;
        }
    }

    /** */
    public void testEmptyMap() throws Exception {
        CachePartitionPartialCountersMap map = CachePartitionPartialCountersMap.EMPTY;

        assertFalse(map.remove(1));

        map.trim();

        assertNotNull(map.toString());
    }
}
