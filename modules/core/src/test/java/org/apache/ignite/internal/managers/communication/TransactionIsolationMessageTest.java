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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class TransactionIsolationMessageTest {
    /** */
    @Test
    public void testTransactionIsolationCode() {
        assertEquals(-1, new TransactionIsolationMessage(null).code());
        assertEquals(0, new TransactionIsolationMessage(READ_COMMITTED).code());
        assertEquals(1, new TransactionIsolationMessage(REPEATABLE_READ).code());
        assertEquals(2, new TransactionIsolationMessage(SERIALIZABLE).code());

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            assertTrue(new TransactionIsolationMessage(isolation).code() >= 0);
            assertTrue(new TransactionIsolationMessage(isolation).code() < 3);
        }
    }

    /** */
    @Test
    public void testTransactionIsolationFromCode() {
        TransactionIsolationMessage msg = new TransactionIsolationMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(READ_COMMITTED, msg.value());

        msg.code((byte)1);
        assertSame(REPEATABLE_READ, msg.value());

        msg.code((byte)2);
        assertSame(SERIALIZABLE, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)3), IllegalArgumentException.class);
        assertEquals("Unknown transaction isolation code: 3", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (TransactionIsolation isolation : F.concat(TransactionIsolation.values(), (TransactionIsolation)null)) {
            TransactionIsolationMessage msg = new TransactionIsolationMessage(isolation);

            assertEquals(isolation, msg.value());

            TransactionIsolationMessage newMsg = new TransactionIsolationMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }

    /** */
    @Test
    public void testIs() {
        assertTrue(new TransactionIsolationMessage().is(null));
        assertTrue(new TransactionIsolationMessage(null).is(null));
        assertTrue(new TransactionIsolationMessage(READ_COMMITTED).is(READ_COMMITTED));
        assertFalse(new TransactionIsolationMessage(READ_COMMITTED).is(SERIALIZABLE));
    }
}
