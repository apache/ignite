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

import java.util.Arrays;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests the packed counter storage of {@link PartitionUpdateCountersMessage}. */
public class PartitionUpdateCountersMessageTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ID = 42;

    /** */
    @Test
    public void testItemsAreReadBackAsWritten() {
        PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(CACHE_ID, 3);

        msg.add(1, 100L, 5L);
        msg.add(2, Long.MAX_VALUE, 1L);
        msg.add(Integer.MAX_VALUE, 0L, Long.MAX_VALUE);

        assertEquals(CACHE_ID, msg.cacheId());
        assertEquals(3, msg.size());

        assertEquals(1, msg.partition(0));
        assertEquals(100L, msg.initialCounter(0));
        assertEquals(5L, msg.updatesCount(0));

        assertEquals(2, msg.partition(1));
        assertEquals(Long.MAX_VALUE, msg.initialCounter(1));
        assertEquals(1L, msg.updatesCount(1));

        assertEquals(Integer.MAX_VALUE, msg.partition(2));
        assertEquals(0L, msg.initialCounter(2));
        assertEquals(Long.MAX_VALUE, msg.updatesCount(2));
    }

    /**
     * Adding past the initial size must grow the storage. Growth by a factor alone falls short of the request for a
     * small array, and the write then lands outside it.
     */
    @Test
    public void testAddPastInitialSize() {
        PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(CACHE_ID, 1);

        for (int i = 0; i < 64; i++)
            msg.add(i, i * 10L, i * 100L);

        assertEquals(64, msg.size());

        for (int i = 0; i < 64; i++) {
            assertEquals(i, msg.partition(i));
            assertEquals(i * 10L, msg.initialCounter(i));
            assertEquals(i * 100L, msg.updatesCount(i));
        }
    }

    /** The wire form must not depend on the byte order of the host, so the layout is asserted byte by byte. */
    @Test
    public void testWireLayoutIsLittleEndian() {
        PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(CACHE_ID, 1);

        msg.add(0x04030201, 0x0807060504030201L, 0x1817161514131211L);

        byte[] expected = {
            0x01, 0x02, 0x03, 0x04,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18
        };

        assertTrue("Unexpected wire layout", Arrays.equals(expected, msg.data));
    }

    /** */
    @Test
    public void testFinishUpdatingTrimsSpareSpace() {
        PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(CACHE_ID, 8);

        msg.add(1, 1L, 1L);

        msg.finishUpdating();

        assertEquals(20, msg.data.length);

        assertEquals(1, msg.partition(0));
        assertEquals(1L, msg.initialCounter(0));
        assertEquals(1L, msg.updatesCount(0));
    }

    /** */
    @Test
    public void testNextCounterFollowsInitialCounter() {
        PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(CACHE_ID, 2);

        msg.add(7, 30L, 2L);

        assertEquals((Long)31L, msg.nextCounter(7));
        assertEquals((Long)32L, msg.nextCounter(7));

        assertNull(msg.nextCounter(8));
    }
}
