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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class CacheEntryPredicateAdapterMessageTest {
    /** */
    @Test
    public void testCacheEntryPredicateAdapterCode() {
        assertEquals(0, prepare(new CacheEntryPredicateAdapter()));
        assertEquals(0, prepare(new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.OTHER)));
        assertEquals(1, prepare(new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.VALUE)));
        assertEquals(1, prepare(new CacheEntryPredicateAdapter((CacheObject)null)));
        assertEquals(2, prepare(new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.HAS_VALUE)));
        assertEquals(3, prepare(new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.HAS_NO_VALUE)));
        assertEquals(4, prepare(new CacheEntryPredicateAdapter(CacheEntryPredicateAdapter.PredicateType.ALWAYS_FALSE)));

        for (CacheEntryPredicateAdapter.PredicateType t : CacheEntryPredicateAdapter.PredicateType.values()) {
            assertTrue(prepare(new CacheEntryPredicateAdapter(t)) >= 0);
            assertTrue(prepare(new CacheEntryPredicateAdapter(t)) < 5);
        }
    }

    byte prepare(CacheEntryPredicateAdapter msg){
        try {
            msg.prepareMarshal(jdk());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }

        return msg.code();
    }

    /** */
    @Test
    public void testCacheEntryPredicateAdapterFromCode() throws IgniteCheckedException {
        CacheEntryPredicateAdapter msg = new CacheEntryPredicateAdapter((CacheObject)null);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.VALUE, msg.type());

        msg.code((byte)0);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.OTHER, msg.type());

        msg.code((byte)1);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.VALUE, msg.type());

        msg.code((byte)2);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.HAS_VALUE, msg.type());

        msg.code((byte)3);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.HAS_NO_VALUE, msg.type());

        msg.code((byte)4);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(CacheEntryPredicateAdapter.PredicateType.ALWAYS_FALSE, msg.type());

        Throwable t = assertThrowsWithCause(() -> {
            msg.code((byte)5);

            try {
                msg.finishUnmarshal(jdk(), U.gridClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }, IllegalArgumentException.class);
        assertEquals("Unknown cache entry predicate type code: 5", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() throws IgniteCheckedException {
        for (CacheEntryPredicateAdapter.PredicateType t : F.concat(CacheEntryPredicateAdapter.PredicateType.values())) {
            CacheEntryPredicateAdapter msg = new CacheEntryPredicateAdapter(t);

            assertEquals(t, msg.type());

            CacheEntryPredicateAdapter newMsg = new CacheEntryPredicateAdapter();

            msg.prepareMarshal(jdk());

            newMsg.code(msg.code());

            newMsg.finishUnmarshal(jdk(), U.gridClassLoader());

            assertEquals(msg.type(), newMsg.type());
        }
    }
}
