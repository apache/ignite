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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class CacheOperationModeMessageTest {
    /** */
    @Test
    public void testCacheOperationModeCode() {
        assertEquals(-1, new GridCacheOperationMessage(null).code());
        assertEquals(0, new GridCacheOperationMessage(GridCacheOperation.READ).code());
        assertEquals(1, new GridCacheOperationMessage(GridCacheOperation.CREATE).code());
        assertEquals(2, new GridCacheOperationMessage(GridCacheOperation.UPDATE).code());
        assertEquals(3, new GridCacheOperationMessage(GridCacheOperation.DELETE).code());
        assertEquals(4, new GridCacheOperationMessage(GridCacheOperation.TRANSFORM).code());
        assertEquals(5, new GridCacheOperationMessage(GridCacheOperation.RELOAD).code());
        assertEquals(6, new GridCacheOperationMessage(GridCacheOperation.NOOP).code());

        for (GridCacheOperation op : GridCacheOperation.values()) {
            assertTrue(new GridCacheOperationMessage(op).code() != -1);
            assertTrue(new GridCacheOperationMessage(op).code() >= 0);
            assertTrue(new GridCacheOperationMessage(op).code() < 7);
        }
    }

    /** */
    @Test
    public void testCacheOperationModeFromCode() {
        GridCacheOperationMessage msg = new GridCacheOperationMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(GridCacheOperation.READ, msg.value());

        msg.code((byte)1);
        assertSame(GridCacheOperation.CREATE, msg.value());

        msg.code((byte)2);
        assertSame(GridCacheOperation.UPDATE, msg.value());

        msg.code((byte)3);
        assertSame(GridCacheOperation.DELETE, msg.value());

        msg.code((byte)4);
        assertSame(GridCacheOperation.TRANSFORM, msg.value());

        msg.code((byte)5);
        assertSame(GridCacheOperation.RELOAD, msg.value());

        msg.code((byte)6);
        assertSame(GridCacheOperation.NOOP, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)7), IllegalArgumentException.class);
        assertEquals("Unknown cache operation code: 7", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (GridCacheOperation op : F.concat(GridCacheOperation.values(), (GridCacheOperation)null)) {
            GridCacheOperationMessage msg = new GridCacheOperationMessage(op);

            assertEquals(op, msg.value());

            GridCacheOperationMessage newMsg = new GridCacheOperationMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }
}
