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

package org.apache.ignite.lang;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "Lang")
public class GridByteArrayListSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testCapacity() {
        int cap = 10;

        GridByteArrayList list = new GridByteArrayList(cap);

        byte[] arr = list.internalArray();

        assert arr != null;
        assert arr.length == cap;

        arr = list.array();

        assert arr != null;
        assert arr.length == 0;

        list.add((byte)0x01);

        list.allocate(cap);

        arr = list.internalArray();

        assert arr != null;
        assert arr.length == (cap + 1);

        assert list.size() == 1;
    }

    /**
     *
     */
    public void testAddSetByte() {
        GridByteArrayList list = new GridByteArrayList(10);

        byte a = 0x01;

        list.add(a);

        assert list.get(0) == a;

        byte b = 0x02;

        list.set(0, b);

        assert list.get(0) == b;
    }

    /**
     *
     */
    public void testAddSetInteger() {
        GridByteArrayList list = new GridByteArrayList(10);

        int num = 1;

        list.add(num);

        assert list.size() == 4;

        assert Arrays.equals(list.array(), U.intToBytes(num));

        int num2 = 2;

        list.add(num2);

        int num3 = 3;

        list.set(4, num3);

        assert list.size() == 8;

        byte[] arr2 = new byte[8];

        U.arrayCopy(U.intToBytes(num), 0, arr2, 0, 4);
        U.arrayCopy(U.intToBytes(num3), 0, arr2, 4, 4);

        assert Arrays.equals(list.array(), arr2);

        assert list.getInt(0) == num;
        assert list.getInt(4) == num3;
    }

    /**
     *
     */
    public void testAddByteArray() {
        GridByteArrayList list = new GridByteArrayList(3);

        list.add((byte)0x01);

        byte[] arr = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};

        list.add(arr, 0, arr.length);

        assert list.size() == 6;

        byte[] arr2 = new byte[] {0x01, 0x01, 0x02, 0x03, 0x04, 0x05};

        assert Arrays.equals(list.array(), arr2);

        // Capacity should grow.
        assert list.internalArray().length == arr2.length * 2;
    }

    /**
     *
     */
    public void testAddByteBuffer() {
        GridByteArrayList list = new GridByteArrayList(3);

        list.add((byte)0x01);

        ByteBuffer buf = ByteBuffer.allocateDirect(5);

        buf.put(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });

        buf.rewind();

        list.add(buf, 5);

        assert list.size() == 6;

        byte[] arr2 = new byte[] { 0x01, 0x01, 0x02, 0x03, 0x04, 0x05 };

        assert Arrays.equals(list.array(), arr2);

        // Capacity should grow.
        assert list.internalArray().length == arr2.length * 2;
    }

    /**
     *
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    public void testBounds()  {
        GridByteArrayList list = new GridByteArrayList(3);

        try {
            list.get(0);

            assert false;
        }
        catch (AssertionError e) {
            info("Caught expected error: " + e);
        }

        try {
            list.set(0, (byte)0x01);

            assert false;
        }
        catch (AssertionError e) {
            info("Caught expected error: " + e);
        }

        try {
            list.getInt(0);

            assert false;
        }
        catch (AssertionError e) {
            info("Caught expected error: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRead() throws Exception {
        GridByteArrayList list = new GridByteArrayList(10);

        byte[] arr = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };

        InputStream in = new ByteArrayInputStream(arr);

        list.readAll(in);

        assert Arrays.equals(list.array(), arr);
    }
}