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

package org.apache.ignite.internal.util.io;

import java.io.ByteArrayInputStream;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.GridTestIoUtils.getCharByByteLE;
import static org.apache.ignite.GridTestIoUtils.getDoubleByByteLE;
import static org.apache.ignite.GridTestIoUtils.getFloatByByteLE;
import static org.apache.ignite.GridTestIoUtils.getIntByByteLE;
import static org.apache.ignite.GridTestIoUtils.getLongByByteLE;
import static org.apache.ignite.GridTestIoUtils.getShortByByteLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Grid unsafe data input/output byte order sanity tests.
 */
public class GridUnsafeDataInputOutputByteOrderSelfTest {
    /** Array length. */
    private static final int ARR_LEN = 16;

    /** Length bytes. */
    private static final int LEN_BYTES = 4;

    /** Rnd. */
    private static Random RND = new Random();

    /** Out. */
    private GridUnsafeDataOutput out;

    /** In. */
    private GridUnsafeDataInput in;

    /** */
    @Before
    public void setUp() throws Exception {
        out = new GridUnsafeDataOutput(16 * 8 + LEN_BYTES);
        in = new GridUnsafeDataInput();
        in.inputStream(new ByteArrayInputStream(out.internalArray()));
    }

    /** */
    @After
    public void tearDown() throws Exception {
        in.close();
        out.close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        short val = (short)RND.nextLong();

        out.writeShort(val);

        assertEquals(val, getShortByByteLE(out.internalArray()));
        assertEquals(val, in.readShort());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShortArray() throws Exception {
        short[] arr = new short[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (short)RND.nextLong();

        out.writeShortArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getShortByByteLE(outArr, i * 2 + LEN_BYTES));

        assertArrayEquals(arr, in.readShortArray());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChar() throws Exception {
        char val = (char)RND.nextLong();

        out.writeChar(val);

        assertEquals(val, getCharByByteLE(out.internalArray()));
        assertEquals(val, in.readChar());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharArray() throws Exception {
        char[] arr = new char[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (char)RND.nextLong();

        out.writeCharArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getCharByByteLE(outArr, i * 2 + LEN_BYTES));

        assertArrayEquals(arr, in.readCharArray());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInt() throws Exception {
        int val = RND.nextInt();

        out.writeInt(val);

        assertEquals(val, getIntByByteLE(out.internalArray()));
        assertEquals(val, in.readInt());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIntArray() throws Exception {
        int[] arr = new int[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextInt();

        out.writeIntArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getIntByByteLE(outArr, i * 4 + LEN_BYTES));

        assertArrayEquals(arr, in.readIntArray());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        long val = RND.nextLong();

        out.writeLong(val);

        assertEquals(val, getLongByByteLE(out.internalArray()));
        assertEquals(val, in.readLong());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongArray() throws Exception {
        long[] arr = new long[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextLong();

        out.writeLongArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getLongByByteLE(outArr, i * 8 + LEN_BYTES));

        assertArrayEquals(arr, in.readLongArray());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        float val = RND.nextFloat();

        out.writeFloat(val);

        assertEquals(val, getFloatByByteLE(out.internalArray()), 0);
        assertEquals(val, in.readFloat(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloatArray() throws Exception {
        float[] arr = new float[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextFloat();

        out.writeFloatArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getFloatByByteLE(outArr, i * 4 + LEN_BYTES), 0);

        assertArrayEquals(arr, in.readFloatArray(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        double val = RND.nextDouble();

        out.writeDouble(val);

        assertEquals(val, getDoubleByByteLE(out.internalArray()), 0);
        assertEquals(val, in.readDouble(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleArray() throws Exception {
        double[] arr = new double[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextDouble();

        out.writeDoubleArray(arr);

        byte[] outArr = out.internalArray();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getDoubleByByteLE(outArr, i * 8 + LEN_BYTES), 0);

        assertArrayEquals(arr, in.readDoubleArray(), 0);
    }
}
