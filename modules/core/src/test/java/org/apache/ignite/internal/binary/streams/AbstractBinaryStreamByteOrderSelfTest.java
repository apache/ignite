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

package org.apache.ignite.internal.binary.streams;

import java.util.Random;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.GridTestIoUtils.getCharByByteLE;
import static org.apache.ignite.GridTestIoUtils.getDoubleByByteLE;
import static org.apache.ignite.GridTestIoUtils.getFloatByByteLE;
import static org.apache.ignite.GridTestIoUtils.getIntByByteLE;
import static org.apache.ignite.GridTestIoUtils.getLongByByteLE;
import static org.apache.ignite.GridTestIoUtils.getShortByByteLE;

/**
 * Binary input/output streams byte order sanity tests.
 */
public abstract class AbstractBinaryStreamByteOrderSelfTest extends GridCommonAbstractTest {
    /** Array length. */
    protected static final int ARR_LEN = 16;

    /** Rnd. */
    private static final Random RND = new Random();

    /** Out. */
    protected BinaryAbstractOutputStream out;

    /** In. */
    protected BinaryAbstractInputStream in;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        init();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        out.close();
    }

    /**
     * Initializes streams.
     */
    protected abstract void init();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        short val = (short)RND.nextLong();

        reset();

        out.unsafeWriteShort(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readShort());

        reset();

        out.unsafeWriteShort(0, val);
        out.shift(2);

        checkValueLittleEndian(val);
        assertEquals(val, in.readShortFast());

        reset();

        out.writeShortFast(val);
        out.shift(2);

        checkValueLittleEndian(val);
        assertEquals(val, in.readShortPositioned(0));

        reset();

        out.writeShort(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readShortPositioned(0));

        reset();

        out.writeShort(0, val);
        out.shift(2);

        checkValueLittleEndian(val);
        assertEquals(val, in.readShortPositioned(0));
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getShortByByteLE(outArr, i * 2));

        Assert.assertArrayEquals(arr, in.readShortArray(ARR_LEN));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChar() throws Exception {
        char val = (char)RND.nextLong();

        reset();

        out.unsafeWriteChar(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readChar());

        reset();

        out.writeCharFast(val);
        out.shift(2);

        checkValueLittleEndian(val);
        assertEquals(val, in.readCharFast());

        reset();

        out.writeChar(val);

        checkValueLittleEndian(val);
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getCharByByteLE(outArr, i * 2));

        Assert.assertArrayEquals(arr, in.readCharArray(ARR_LEN));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInt() throws Exception {
        int val = RND.nextInt();

        reset();

        out.unsafeWriteInt(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readInt());

        reset();

        out.unsafeWriteInt(0, val);
        out.shift(4);

        checkValueLittleEndian(val);
        assertEquals(val, in.readIntFast());

        reset();

        out.writeIntFast(val);
        out.shift(4);

        checkValueLittleEndian(val);
        assertEquals(val, in.readIntPositioned(0));

        reset();

        out.writeInt(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readIntPositioned(0));

        reset();

        out.writeInt(0, val);
        out.shift(4);

        checkValueLittleEndian(val);
        assertEquals(val, in.readIntPositioned(0));

        reset();

        out.writeIntArray(new int[] {val});

        checkValueLittleEndian(val);
        assertEquals(val, in.readIntArray(1)[0]);
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getIntByByteLE(outArr, i * 4));

        Assert.assertArrayEquals(arr, in.readIntArray(ARR_LEN));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        long val = RND.nextLong();

        reset();

        out.unsafeWriteLong(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readLong());

        reset();

        out.writeLongFast(val);
        out.shift(8);

        checkValueLittleEndian(val);
        assertEquals(val, in.readLongFast());

        reset();

        out.writeLong(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readLong());

        reset();

        out.writeLongArray(new long[] {val});

        checkValueLittleEndian(val);
        assertEquals(val, in.readLongArray(1)[0]);
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getLongByByteLE(outArr, i * 8));

        Assert.assertArrayEquals(arr, in.readLongArray(ARR_LEN));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        float val = RND.nextFloat();

        reset();

        out.unsafeWriteFloat(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readFloat(), 0);

        reset();

        out.writeFloat(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readFloat(), 0);

        reset();

        out.writeFloatArray(new float[] {val});

        checkValueLittleEndian(val);
        assertEquals(val, in.readFloatArray(1)[0], 0);
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getFloatByByteLE(outArr, i * 4), 0);

        Assert.assertArrayEquals(arr, in.readFloatArray(ARR_LEN), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        double val = RND.nextDouble();

        reset();

        out.unsafeWriteDouble(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readDouble(), 0);

        reset();

        out.writeDouble(val);

        checkValueLittleEndian(val);
        assertEquals(val, in.readDouble(), 0);

        reset();

        out.writeDoubleArray(new double[] {val});

        checkValueLittleEndian(val);
        assertEquals(val, in.readDoubleArray(1)[0], 0);
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

        byte[] outArr = array();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getDoubleByByteLE(outArr, i * 8), 0);

        Assert.assertArrayEquals(arr, in.readDoubleArray(ARR_LEN), 0);
    }

    /**
     *
     */
    private void reset() {
        in.position(0);
        out.position(0);
    }

    /**
     *
     */
    private byte[] array() {
        return out.array();
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(short exp) {
        byte[] arr = array();

        assertEquals(exp, getShortByByteLE(arr));
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(char exp) {
        byte[] arr = array();

        assertEquals(exp, getCharByByteLE(arr));
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(int exp) {
        byte[] arr = array();

        assertEquals(exp, getIntByByteLE(arr));
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(long exp) {
        byte[] arr = array();

        assertEquals(exp, getLongByByteLE(arr));
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(float exp) {
        byte[] arr = array();

        assertEquals(exp, getFloatByByteLE(arr), 0);
    }

    /**
     * @param exp Expected.
     */
    private void checkValueLittleEndian(double exp) {
        byte[] arr = array();

        assertEquals(exp, getDoubleByByteLE(arr), 0);
    }
}
