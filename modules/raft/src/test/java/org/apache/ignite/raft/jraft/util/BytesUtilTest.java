/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BytesUtilTest {

    @Test
    public void testNullToEmpty() {
        assertArrayEquals(new byte[] {}, BytesUtil.nullToEmpty(null));
        assertArrayEquals(new byte[] {1, 2}, BytesUtil.nullToEmpty(new byte[] {1, 2}));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testIsEmpty() {
        assertTrue(BytesUtil.isEmpty(null));

        assertFalse(BytesUtil.isEmpty(new byte[] {1, 2}));
    }

//    @Test
//    public void testWriteUtf8() {
//        assertNull(BytesUtil.writeUtf8(null));
//
//        assertArrayEquals(new byte[] { 102, 111, 111 }, BytesUtil.writeUtf8("foo"));
//    }
//
//    @Test
//    public void testReadUtf8() {
//        assertNull(BytesUtil.readUtf8(null));
//
//        assertEquals("foo", BytesUtil.readUtf8(new byte[] { 102, 111, 111 }));
//    }

    @Test
    public void testNextBytes() {
        assertArrayEquals(new byte[] {0}, BytesUtil.nextBytes(new byte[] {}));
        assertArrayEquals(new byte[] {1, 2, 0}, BytesUtil.nextBytes(new byte[] {1, 2}));
    }

    @Test
    public void testCompare() {
        byte[] array = new byte[] {1, 2};

        assertEquals(0, BytesUtil.compare(array, array));
        assertEquals(-2, BytesUtil.compare(new byte[] {1, 2}, new byte[] {3, 4}));
        assertEquals(0, BytesUtil.compare(new byte[] {3, 4}, new byte[] {3, 4}));
    }

    @Test
    public void testMax() {
        byte[] array = new byte[] {3, 4};

        assertArrayEquals(array, BytesUtil.max(array, array));
        assertArrayEquals(array, BytesUtil.max(new byte[] {1, 2}, array));
    }

    @Test
    public void testMin() {
        byte[] array = new byte[] {1, 2};
        assertArrayEquals(array, BytesUtil.min(array, array));
        assertArrayEquals(array, BytesUtil.min(array, new byte[] {3, 4}));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testToHex() {
        assertNull(BytesUtil.toHex(null));

        assertEquals("0102", BytesUtil.toHex(new byte[] {1, 2}));
    }

    @Test
    public void testHexStringToByteArray() {
        assertNull(BytesUtil.hexStringToByteArray(null));

        assertArrayEquals(new byte[] {-17, -5}, BytesUtil.hexStringToByteArray("foob"));
    }

//    @Test
//    public void toUtf8BytesTest() {
//        for (int i = 0; i < 100000; i++) {
//            String in = UUID.randomUUID().toString();
//            assertArrayEquals(Utils.getBytes(in), BytesUtil.writeUtf8(in));
//        }
//    }
//
//    @Test
//    public void toUtf8StringTest() {
//        for (int i = 0; i < 100000; i++) {
//            String str = UUID.randomUUID().toString();
//            byte[] in = Utils.getBytes(str);
//            assertEquals(new String(in, StandardCharsets.UTF_8), BytesUtil.readUtf8(in));
//        }
//    }

    @Test
    public void hexTest() {
        final String text = "Somebody save your soul cause you've been sinning in this city I know";
        final String hexString = BytesUtil.toHex(text.getBytes());
        System.out.println(hexString);
        final byte[] bytes = BytesUtil.hexStringToByteArray(hexString);
        assertEquals(text, new String(bytes));
    }
}
