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

import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class IndexKeyTypeMessageTest {
    /** */
    @Test
    public void testIndexKeyTypeCode() {
        assertEquals(Byte.MIN_VALUE, new IndexKeyTypeMessage(null).code());
        assertEquals(-1, new IndexKeyTypeMessage(IndexKeyType.UNKNOWN).code());
        assertEquals(0, new IndexKeyTypeMessage(IndexKeyType.NULL).code());
        assertEquals(1, new IndexKeyTypeMessage(IndexKeyType.BOOLEAN).code());
        assertEquals(2, new IndexKeyTypeMessage(IndexKeyType.BYTE).code());
        assertEquals(3, new IndexKeyTypeMessage(IndexKeyType.SHORT).code());
        assertEquals(4, new IndexKeyTypeMessage(IndexKeyType.INT).code());
        assertEquals(5, new IndexKeyTypeMessage(IndexKeyType.LONG).code());
        assertEquals(6, new IndexKeyTypeMessage(IndexKeyType.DECIMAL).code());
        assertEquals(7, new IndexKeyTypeMessage(IndexKeyType.DOUBLE).code());
        assertEquals(8, new IndexKeyTypeMessage(IndexKeyType.FLOAT).code());
        assertEquals(9, new IndexKeyTypeMessage(IndexKeyType.TIME).code());
        assertEquals(10, new IndexKeyTypeMessage(IndexKeyType.DATE).code());
        assertEquals(11, new IndexKeyTypeMessage(IndexKeyType.TIMESTAMP).code());
        assertEquals(12, new IndexKeyTypeMessage(IndexKeyType.BYTES).code());
        assertEquals(13, new IndexKeyTypeMessage(IndexKeyType.STRING).code());
        assertEquals(14, new IndexKeyTypeMessage(IndexKeyType.STRING_IGNORECASE).code());
        assertEquals(15, new IndexKeyTypeMessage(IndexKeyType.BLOB).code());
        assertEquals(16, new IndexKeyTypeMessage(IndexKeyType.CLOB).code());
        assertEquals(17, new IndexKeyTypeMessage(IndexKeyType.ARRAY).code());
        assertEquals(18, new IndexKeyTypeMessage(IndexKeyType.RESULT_SET).code());
        assertEquals(19, new IndexKeyTypeMessage(IndexKeyType.JAVA_OBJECT).code());
        assertEquals(20, new IndexKeyTypeMessage(IndexKeyType.UUID).code());
        assertEquals(21, new IndexKeyTypeMessage(IndexKeyType.STRING_FIXED).code());
        assertEquals(22, new IndexKeyTypeMessage(IndexKeyType.GEOMETRY).code());
        assertEquals(24, new IndexKeyTypeMessage(IndexKeyType.TIMESTAMP_TZ).code());
        assertEquals(25, new IndexKeyTypeMessage(IndexKeyType.ENUM).code());

        for (IndexKeyType keyType : IndexKeyType.values())
            assertTrue(new IndexKeyTypeMessage(keyType).code() != IndexKeyTypeMessage.NULL_VALUE_CODE);
    }

    /** */
    @Test
    public void testIndexKeyTypeFromCode() {
        IndexKeyTypeMessage msg = new IndexKeyTypeMessage(null);

        msg.code(IndexKeyTypeMessage.NULL_VALUE_CODE);
        assertNull(msg.value());

        msg.code((byte)-1);
        assertSame(IndexKeyType.UNKNOWN, msg.value());

        msg.code((byte)0);
        assertSame(IndexKeyType.NULL, msg.value());

        msg.code((byte)1);
        assertSame(IndexKeyType.BOOLEAN, msg.value());

        msg.code((byte)2);
        assertSame(IndexKeyType.BYTE, msg.value());

        msg.code((byte)3);
        assertSame(IndexKeyType.SHORT, msg.value());

        msg.code((byte)4);
        assertSame(IndexKeyType.INT, msg.value());

        msg.code((byte)5);
        assertSame(IndexKeyType.LONG, msg.value());

        msg.code((byte)6);
        assertSame(IndexKeyType.DECIMAL, msg.value());

        msg.code((byte)7);
        assertSame(IndexKeyType.DOUBLE, msg.value());

        msg.code((byte)8);
        assertSame(IndexKeyType.FLOAT, msg.value());

        msg.code((byte)9);
        assertSame(IndexKeyType.TIME, msg.value());

        msg.code((byte)10);
        assertSame(IndexKeyType.DATE, msg.value());

        msg.code((byte)11);
        assertSame(IndexKeyType.TIMESTAMP, msg.value());

        msg.code((byte)12);
        assertSame(IndexKeyType.BYTES, msg.value());

        msg.code((byte)13);
        assertSame(IndexKeyType.STRING, msg.value());

        msg.code((byte)14);
        assertSame(IndexKeyType.STRING_IGNORECASE, msg.value());

        msg.code((byte)15);
        assertSame(IndexKeyType.BLOB, msg.value());

        msg.code((byte)16);
        assertSame(IndexKeyType.CLOB, msg.value());

        msg.code((byte)17);
        assertSame(IndexKeyType.ARRAY, msg.value());

        msg.code((byte)18);
        assertSame(IndexKeyType.RESULT_SET, msg.value());

        msg.code((byte)19);
        assertSame(IndexKeyType.JAVA_OBJECT, msg.value());

        msg.code((byte)20);
        assertSame(IndexKeyType.UUID, msg.value());

        msg.code((byte)21);
        assertSame(IndexKeyType.STRING_FIXED, msg.value());

        msg.code((byte)22);
        assertSame(IndexKeyType.GEOMETRY, msg.value());

        msg.code((byte)24);
        assertSame(IndexKeyType.TIMESTAMP_TZ, msg.value());

        msg.code((byte)25);
        assertSame(IndexKeyType.ENUM, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)23), IllegalArgumentException.class);

        assertEquals("Unknown index key type code: " + 23, t.getMessage());

        for (byte c = 26; c >= 26 && c <= Byte.MAX_VALUE; ++c) {
            byte c0 = c;

            t = assertThrowsWithCause(() -> msg.code(c0), IllegalArgumentException.class);

            assertEquals("Unknown index key type code: " + c0, t.getMessage());
        }

        for (byte c = (byte)(IndexKeyTypeMessage.NULL_VALUE_CODE + 1); c < -1; ++c) {
            byte c0 = c;

            t = assertThrowsWithCause(() -> msg.code(c0), IllegalArgumentException.class);

            assertEquals("Unknown index key type code: " + c0, t.getMessage());
        }
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (IndexKeyType keyType : F.concat(IndexKeyType.values(), (IndexKeyType)null)) {
            IndexKeyTypeMessage msg = new IndexKeyTypeMessage(keyType);

            assertEquals(keyType, msg.value());

            IndexKeyTypeMessage newMsg = new IndexKeyTypeMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }
}
