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

import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class CacheEntryPredicateAdapterMessageTest {
    /** */
    @Test
    public void testCacheEntryPredicateAdapterCode() {
        assertEquals(0, new CacheEntryPredicateAdapter().code());
        assertEquals(0, new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.OTHER).code());
        assertEquals(1, new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.VALUE).code());
        assertEquals(1, new CacheEntryPredicateAdapter((CacheObject)null).code());
        assertEquals(2, new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.HAS_VALUE).code());
        assertEquals(3, new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.HAS_NO_VALUE).code());
        assertEquals(4, new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.ALWAYS_FALSE).code());

        for (CacheEntryPredicateAdapter.PredicateType t : CacheEntryPredicateAdapter.PredicateType.values()) {
            assertTrue(new CacheEntryPredicateAdapter(t).code() >= 0);
            assertTrue(new CacheEntryPredicateAdapter(t).code() < 5);
        }
    }

    /** */
    @Test
    public void testCacheEntryPredicateAdapterFromCode() {
        CacheEntryPredicateAdapter msg = new CacheEntryPredicateAdapter((CacheObject)null);
        assertSame(CacheEntryPredicateAdapter.PredicateType.VALUE, msg.type());

        msg.code((byte)0);
        assertSame(CacheEntryPredicateAdapter.PredicateType.OTHER, msg.type());

        msg.code((byte)1);
        assertSame(CacheEntryPredicateAdapter.PredicateType.VALUE, msg.type());

        msg.code((byte)2);
        assertSame(CacheEntryPredicateAdapter.PredicateType.HAS_VALUE, msg.type());

        msg.code((byte)3);
        assertSame(CacheEntryPredicateAdapter.PredicateType.HAS_NO_VALUE, msg.type());

        msg.code((byte)4);
        assertSame(CacheEntryPredicateAdapter.PredicateType.ALWAYS_FALSE, msg.type());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)5), IllegalArgumentException.class);
        assertEquals("Unknown cache entry predicate type code: 5", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (CacheEntryPredicateAdapter.PredicateType t : F.concat(CacheEntryPredicateAdapter.PredicateType.values())) {
            CacheEntryPredicateAdapter msg = new CacheEntryPredicateAdapter(t);

            assertEquals(t, msg.type());

            CacheEntryPredicateAdapter newMsg = new CacheEntryPredicateAdapter();
            newMsg.code(msg.code());

            assertEquals(msg.type(), newMsg.type());
        }
    }
}
