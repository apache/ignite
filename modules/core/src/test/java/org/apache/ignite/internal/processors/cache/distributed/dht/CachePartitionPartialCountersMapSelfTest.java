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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
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
    @Test
    public void testEmptyMap() throws Exception {
        CachePartitionPartialCountersMap map = CachePartitionPartialCountersMap.EMPTY;

        assertFalse(map.remove(1));

        map.trim();

        assertNotNull(map.toString());
    }
}
