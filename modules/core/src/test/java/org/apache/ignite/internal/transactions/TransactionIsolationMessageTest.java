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

package org.apache.ignite.internal.transactions;

import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/** */
public class TransactionIsolationMessageTest {
    /** */
    @Test
    public void testTransactionIsolationCode() {
        assertEquals(-1, new TransactionIsolationMessage(null).code());
        assertEquals(0, new TransactionIsolationMessage(TransactionIsolation.READ_COMMITTED).code());
        assertEquals(1, new TransactionIsolationMessage(TransactionIsolation.REPEATABLE_READ).code());
        assertEquals(2, new TransactionIsolationMessage(TransactionIsolation.SERIALIZABLE).code());
    }

    /** */
    @Test
    public void testTransactionIsolationFromCode() {
        TransactionIsolationMessage msg = new TransactionIsolationMessage(null);

        msg.code((short)-1);
        assertNull(msg.value());

        msg.code((short)0);
        assertSame(TransactionIsolation.READ_COMMITTED, msg.value());

        msg.code((short)1);
        assertSame(TransactionIsolation.REPEATABLE_READ, msg.value());

        msg.code((short)2);
        assertSame(TransactionIsolation.SERIALIZABLE, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((short)3), IllegalArgumentException.class);
        assertEquals("Unknown transaction isolation code: 3", t.getMessage());
    }
}
