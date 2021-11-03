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

package org.apache.ignite.internal.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.schema.row.ExpandableByteBuf;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class ExpandableByteBufTest {
    /**
     *
     */
    @Test
    public void allTypesDirectOrder() throws Exception {
        ExpandableByteBuf buf = new ExpandableByteBuf(5);

        byte[] targetBytes = {1, 2, 3, 4, 5, 6, 7};
        String targetStr = "abcdefg";

        buf.put(0, (byte) 1);
        buf.putShort(1, (short) 2);
        buf.putInt(3, 3);
        buf.putLong(7, 4L);
        buf.putFloat(15, 5.f);
        buf.putDouble(19, 6.d);
        buf.putBytes(27, targetBytes);
        buf.putString(34, targetStr, StandardCharsets.UTF_8.newEncoder());

        byte[] arr = buf.toArray();
        assertEquals(41, arr.length);

        ByteBuffer b = ByteBuffer.wrap(arr);
        b.order(ByteOrder.LITTLE_ENDIAN);

        assertEquals((byte) 1, b.get(0));
        assertEquals((short) 2, b.getShort(1));
        assertEquals(3, b.getInt(3));
        assertEquals(4L, b.getLong(7));
        assertEquals(5.f, b.getFloat(15));
        assertEquals(6.d, b.getDouble(19));

        byte[] bytes = new byte[7];
        b.position(27);
        b.get(bytes);

        assertArrayEquals(targetBytes, bytes);

        b.position(34);
        b.get(bytes);

        assertEquals(targetStr, new String(bytes, StandardCharsets.UTF_8));
    }

    /**
     *
     */
    @Test
    public void allTypesReverseOrder() throws Exception {
        ExpandableByteBuf buf = new ExpandableByteBuf(5);

        byte[] targetBytes = {1, 2, 3, 4, 5, 6, 7};
        String targetStr = "abcdefg";

        buf.putString(34, targetStr, StandardCharsets.UTF_8.newEncoder());
        buf.putBytes(27, targetBytes);
        buf.putDouble(19, 6.d);
        buf.putFloat(15, 5.f);
        buf.putLong(7, 4L);
        buf.putInt(3, 3);
        buf.putShort(1, (short) 2);
        buf.put(0, (byte) 1);

        byte[] arr = buf.toArray();
        assertEquals(41, arr.length);

        ByteBuffer b = ByteBuffer.wrap(arr);
        b.order(ByteOrder.LITTLE_ENDIAN);

        assertEquals((byte) 1, b.get(0));
        assertEquals((short) 2, b.getShort(1));
        assertEquals(3, b.getInt(3));
        assertEquals(4L, b.getLong(7));
        assertEquals(5.f, b.getFloat(15));
        assertEquals(6.d, b.getDouble(19));

        byte[] bytes = new byte[7];
        b.position(27);
        b.get(bytes);

        assertArrayEquals(targetBytes, bytes);

        b.position(34);
        b.get(bytes);

        assertEquals(targetStr, new String(bytes, StandardCharsets.UTF_8));
    }

    /**
     *
     */
    @Test
    public void exampleFromJavadoc() {
        ExpandableByteBuf b = new ExpandableByteBuf(1);
        b.put(0, (byte) 1); // Does not expand.
        b.put(5, (byte) 1); // Expands, meaningful bytes are [0..5]
        byte[] data = b.toArray();

        assertEquals(6, data.length);
    }

    /**
     *
     */
    @Test
    public void stringExpandMultipleTimes() throws Exception {
        // Expansion chain 1->2->4->8->16.
        ExpandableByteBuf buf = new ExpandableByteBuf(1);

        assertEquals(3, buf.putString(0, "我", StandardCharsets.UTF_8.newEncoder()));
        assertEquals("我", new String(buf.toArray()));

        assertEquals(3, buf.putString(3, "愛", StandardCharsets.UTF_8.newEncoder()));
        assertEquals("我愛", new String(buf.toArray()));

        assertEquals(4, buf.putString(6, "Java", StandardCharsets.UTF_8.newEncoder()));
        assertEquals(10, buf.toArray().length);
        assertEquals("我愛Java", new String(buf.toArray()));
    }

    /**
     *
     */
    @Test
    public void stringWithnMultiByteChars() throws Exception {
        // Expansion chain 1->2->4.
        ExpandableByteBuf buf = new ExpandableByteBuf(1);

        String str = "abcdefghijklmnopq";

        buf.putString(0, str, StandardCharsets.UTF_8.newEncoder());

        byte[] arr = buf.toArray();

        assertEquals(str.length(), arr.length);
        assertEquals(str, new String(arr));
    }
}
