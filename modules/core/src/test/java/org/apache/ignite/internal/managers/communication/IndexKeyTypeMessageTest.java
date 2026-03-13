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
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
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
        assertEquals(Byte.MIN_VALUE, prepare(new IndexKeyTypeMessage(null)));
        assertEquals(-1, prepare(new IndexKeyTypeMessage(IndexKeyType.UNKNOWN)));
        assertEquals(0, prepare(new IndexKeyTypeMessage(IndexKeyType.NULL)));
        assertEquals(1, prepare(new IndexKeyTypeMessage(IndexKeyType.BOOLEAN)));
        assertEquals(2, prepare(new IndexKeyTypeMessage(IndexKeyType.BYTE)));
        assertEquals(3, prepare(new IndexKeyTypeMessage(IndexKeyType.SHORT)));
        assertEquals(4, prepare(new IndexKeyTypeMessage(IndexKeyType.INT)));
        assertEquals(5, prepare(new IndexKeyTypeMessage(IndexKeyType.LONG)));
        assertEquals(6, prepare(new IndexKeyTypeMessage(IndexKeyType.DECIMAL)));
        assertEquals(7, prepare(new IndexKeyTypeMessage(IndexKeyType.DOUBLE)));
        assertEquals(8, prepare(new IndexKeyTypeMessage(IndexKeyType.FLOAT)));
        assertEquals(9, prepare(new IndexKeyTypeMessage(IndexKeyType.TIME)));
        assertEquals(10, prepare(new IndexKeyTypeMessage(IndexKeyType.DATE)));
        assertEquals(11, prepare(new IndexKeyTypeMessage(IndexKeyType.TIMESTAMP)));
        assertEquals(12, prepare(new IndexKeyTypeMessage(IndexKeyType.BYTES)));
        assertEquals(13, prepare(new IndexKeyTypeMessage(IndexKeyType.STRING)));
        assertEquals(14, prepare(new IndexKeyTypeMessage(IndexKeyType.STRING_IGNORECASE)));
        assertEquals(15, prepare(new IndexKeyTypeMessage(IndexKeyType.BLOB)));
        assertEquals(16, prepare(new IndexKeyTypeMessage(IndexKeyType.CLOB)));
        assertEquals(17, prepare(new IndexKeyTypeMessage(IndexKeyType.ARRAY)));
        assertEquals(18, prepare(new IndexKeyTypeMessage(IndexKeyType.RESULT_SET)));
        assertEquals(19, prepare(new IndexKeyTypeMessage(IndexKeyType.JAVA_OBJECT)));
        assertEquals(20, prepare(new IndexKeyTypeMessage(IndexKeyType.UUID)));
        assertEquals(21, prepare(new IndexKeyTypeMessage(IndexKeyType.STRING_FIXED)));
        assertEquals(22, prepare(new IndexKeyTypeMessage(IndexKeyType.GEOMETRY)));
        assertEquals(24, prepare(new IndexKeyTypeMessage(IndexKeyType.TIMESTAMP_TZ)));
        assertEquals(25, prepare(new IndexKeyTypeMessage(IndexKeyType.ENUM)));

        for (IndexKeyType keyType : IndexKeyType.values())
            assertTrue(prepare(new IndexKeyTypeMessage(keyType)) != IndexKeyTypeMessage.NULL_VALUE_CODE);
    }

    byte prepare(IndexKeyTypeMessage msg){
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
    public void testIndexKeyTypeFromCode() throws IgniteCheckedException {
        IndexKeyTypeMessage msg = new IndexKeyTypeMessage(null);

        msg.code(IndexKeyTypeMessage.NULL_VALUE_CODE);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertNull(msg.value());

        msg.code((byte)-1);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.UNKNOWN, msg.value());

        msg.code((byte)0);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.NULL, msg.value());

        msg.code((byte)1);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.BOOLEAN, msg.value());

        msg.code((byte)2);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.BYTE, msg.value());

        msg.code((byte)3);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.SHORT, msg.value());

        msg.code((byte)4);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.INT, msg.value());

        msg.code((byte)5);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.LONG, msg.value());

        msg.code((byte)6);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.DECIMAL, msg.value());

        msg.code((byte)7);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.DOUBLE, msg.value());

        msg.code((byte)8);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.FLOAT, msg.value());

        msg.code((byte)9);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.TIME, msg.value());

        msg.code((byte)10);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.DATE, msg.value());

        msg.code((byte)11);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.TIMESTAMP, msg.value());

        msg.code((byte)12);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.BYTES, msg.value());

        msg.code((byte)13);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.STRING, msg.value());

        msg.code((byte)14);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.STRING_IGNORECASE, msg.value());

        msg.code((byte)15);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.BLOB, msg.value());

        msg.code((byte)16);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.CLOB, msg.value());

        msg.code((byte)17);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.ARRAY, msg.value());

        msg.code((byte)18);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.RESULT_SET, msg.value());

        msg.code((byte)19);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.JAVA_OBJECT, msg.value());

        msg.code((byte)20);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.UUID, msg.value());

        msg.code((byte)21);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.STRING_FIXED, msg.value());

        msg.code((byte)22);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.GEOMETRY, msg.value());

        msg.code((byte)24);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.TIMESTAMP_TZ, msg.value());

        msg.code((byte)25);
        msg.finishUnmarshal(jdk(), U.gridClassLoader());
        assertSame(IndexKeyType.ENUM, msg.value());

        Throwable t = assertThrowsWithCause(() -> {
                msg.code((byte)23);

                try {
                    msg.finishUnmarshal(jdk(), U.gridClassLoader());
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            },
            IllegalArgumentException.class);

        assertEquals("Unknown index key type code: " + 23, t.getMessage());

        for (byte c = 26; c >= 26 && c <= Byte.MAX_VALUE; ++c) {
            byte c0 = c;

            t = assertThrowsWithCause(() -> {
                    msg.code(c0);

                    try {
                        msg.finishUnmarshal(jdk(), U.gridClassLoader());
                    }
                    catch (IgniteCheckedException e) {
                        throw new RuntimeException(e);
                    }
                },
                IllegalArgumentException.class);

            assertEquals("Unknown index key type code: " + c0, t.getMessage());
        }

        for (byte c = (byte)(IndexKeyTypeMessage.NULL_VALUE_CODE + 1); c < -1; ++c) {
            byte c0 = c;

            t = assertThrowsWithCause(() -> {
                msg.code(c0);

                try {
                    msg.finishUnmarshal(jdk(), U.gridClassLoader());
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }, IllegalArgumentException.class);

            assertEquals("Unknown index key type code: " + c0, t.getMessage());
        }
    }

    /** */
    @Test
    public void testConversionConsistency() throws IgniteCheckedException {
        for (IndexKeyType keyType : F.concat(IndexKeyType.values(), (IndexKeyType)null)) {
            IndexKeyTypeMessage msg = new IndexKeyTypeMessage(keyType);

            assertEquals(keyType, msg.value());

            IndexKeyTypeMessage newMsg = new IndexKeyTypeMessage();

            msg.prepareMarshal(jdk());

            newMsg.code(msg.code());

            newMsg.finishUnmarshal(jdk(), U.gridClassLoader());

            assertEquals(msg.value(), newMsg.value());
        }
    }
}
