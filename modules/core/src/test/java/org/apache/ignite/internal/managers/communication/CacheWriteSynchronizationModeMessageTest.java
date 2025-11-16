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

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class CacheWriteSynchronizationModeMessageTest {
    /** */
    @Test
    public void testCacheWriteSynchronizationCode() {
        assertEquals(-1, new CacheWriteSynchronizationModeMessage(null).code());
        assertEquals(0, new CacheWriteSynchronizationModeMessage(FULL_SYNC).code());
        assertEquals(1, new CacheWriteSynchronizationModeMessage(FULL_ASYNC).code());
        assertEquals(2, new CacheWriteSynchronizationModeMessage(PRIMARY_SYNC).code());

        for (CacheWriteSynchronizationMode op : CacheWriteSynchronizationMode.values()) {
            assertTrue(new CacheWriteSynchronizationModeMessage(op).code() >= 0);
            assertTrue(new CacheWriteSynchronizationModeMessage(op).code() < 3);
        }
    }

    /** */
    @Test
    public void testCacheWriteSynchronizationFromCode() {
        CacheWriteSynchronizationModeMessage msg = new CacheWriteSynchronizationModeMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(FULL_SYNC, msg.value());

        msg.code((byte)1);
        assertSame(FULL_ASYNC, msg.value());

        msg.code((byte)2);
        assertSame(PRIMARY_SYNC, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)3), IllegalArgumentException.class);
        assertEquals("Unknown cache write synchronization mode code: 3", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (CacheWriteSynchronizationMode op : F.concat(CacheWriteSynchronizationMode.values(), (CacheWriteSynchronizationMode)null)) {
            CacheWriteSynchronizationModeMessage msg = new CacheWriteSynchronizationModeMessage(op);

            assertEquals(op, msg.value());

            CacheWriteSynchronizationModeMessage newMsg = new CacheWriteSynchronizationModeMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }

    /** */
    @Test
    public void testIs() {
        assertTrue(new CacheWriteSynchronizationModeMessage().is(null));
        assertTrue(new CacheWriteSynchronizationModeMessage(null).is(null));
        assertTrue(new CacheWriteSynchronizationModeMessage(FULL_SYNC).is(FULL_SYNC));
        assertFalse(new CacheWriteSynchronizationModeMessage(FULL_SYNC).is(PRIMARY_SYNC));
    }
}
