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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class TransactionIsolationMessageTest {
    /** */
    @Test
    public void testTransactionIsolationCode() {
        assertEquals(-1, new TransactionIsolationMessage(null).code());
        assertEquals(0, new TransactionIsolationMessage(TransactionIsolation.READ_COMMITTED).code());
        assertEquals(1, new TransactionIsolationMessage(TransactionIsolation.REPEATABLE_READ).code());
        assertEquals(2, new TransactionIsolationMessage(TransactionIsolation.SERIALIZABLE).code());

        for (TransactionIsolation isolation : TransactionIsolation.values())
            assertTrue(new TransactionIsolationMessage(isolation).code() != -1);
    }

    /** */
    @Test
    public void testTransactionIsolationFromCode() {
        TransactionIsolationMessage msg = new TransactionIsolationMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(TransactionIsolation.READ_COMMITTED, msg.value());

        msg.code((byte)1);
        assertSame(TransactionIsolation.REPEATABLE_READ, msg.value());

        msg.code((byte)2);
        assertSame(TransactionIsolation.SERIALIZABLE, msg.value());

        msg.code((byte)3);
        assertSame(null, msg.value());
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
}
