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

package org.apache.ignite.lang.utils;

import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedMap;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link GridBoundedConcurrentOrderedMap}.
 */
@GridCommonTest(group = "Lang")
@RunWith(JUnit4.class)
public class GridBoundedConcurrentOrderedMapSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testEvictionSingleElement() {
        SortedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(Integer.valueOf(10), m.lastKey());
    }

    /**
     *
     */
    @Test
    public void testEvictionListener() {
        GridBoundedConcurrentOrderedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        final AtomicInteger evicted = new AtomicInteger();

        m.evictionListener(new CI2<Integer, String>() {
            @Override public void apply(Integer k, String v) {
                assertEquals(Integer.toString(k), v);
                assertEquals(evicted.getAndIncrement(), k.intValue());
            }
        });

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(10, m.lastKey().intValue());
        assertEquals(10, evicted.get());
    }
}
