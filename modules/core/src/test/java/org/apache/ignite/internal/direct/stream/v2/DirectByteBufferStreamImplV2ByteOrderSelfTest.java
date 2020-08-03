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

package org.apache.ignite.internal.direct.stream.v2;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link DirectByteBufferStreamImplV2} byte order sanity tests.
 */
public class DirectByteBufferStreamImplV2ByteOrderSelfTest {
    /** Array length. */
    private static final int ARR_LEN = 16;

    /** Length bytes. */
    private static final int LEN_BYTES = 1;

    /** Rnd. */
    private static final Random RND = new Random();

    /** Stream. */
    private DirectByteBufferStream stream;

    /** Buff. */
    private ByteBuffer buff;

    /** Array. */
    private byte[] outArr;

    /** */
    @Before
    public void setUp() throws Exception {
        outArr = new byte[ARR_LEN * 8 + LEN_BYTES];

        buff = ByteBuffer.wrap(outArr);

        stream = createStream(buff);
    }

    /**
     * Creates a dummy stream on top of the provided buffer.
     *
     * @param buff Buffer.
     * @return Stream.
     */
    private static DirectByteBufferStreamImplV2 createStream(ByteBuffer buff) {
        DirectByteBufferStreamImplV2 stream = new DirectByteBufferStreamImplV2(new MessageFactory() {
            @Nullable @Override public Message create(short type) {
                return null;
            }
        });

        stream.setBuffer(buff);

        return stream;
    }

    /**
     *
     */
    @Test
    public void testShortArray() {
        short[] arr = new short[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (short)RND.nextLong();

        stream.writeShortArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getShortByByteLE(outArr, i * 2 + LEN_BYTES));

        assertArrayEquals(arr, stream.readShortArray());
    }

    /**
     *
     */
    @Test
    public void testCharArray() {
        char[] arr = new char[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (char)RND.nextLong();

        stream.writeCharArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getCharByByteLE(outArr, i * 2 + LEN_BYTES));

        assertArrayEquals(arr, stream.readCharArray());
    }

    /**
     *
     */
    @Test
    public void testIntArray() {
        int[] arr = new int[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextInt();

        stream.writeIntArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getIntByByteLE(outArr, i * 4 + LEN_BYTES));

        assertArrayEquals(arr, stream.readIntArray());
    }

    /**
     *
     */
    @Test
    public void testLongArray() {
        long[] arr = new long[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextLong();

        stream.writeLongArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getLongByByteLE(outArr, i * 8 + LEN_BYTES));

        assertArrayEquals(arr, stream.readLongArray());
    }

    /**
     *
     */
    @Test
    public void testFloatArray() {
        float[] arr = new float[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextFloat();

        stream.writeFloatArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getFloatByByteLE(outArr, i * 4 + LEN_BYTES), 0);

        assertArrayEquals(arr, stream.readFloatArray(), 0);
    }

    /**
     *
     */
    @Test
    public void testDoubleArray() {
        double[] arr = new double[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextDouble();

        stream.writeDoubleArray(arr);

        buff.rewind();

        for (int i = 0; i < ARR_LEN; i++)
            assertEquals(arr[i], getDoubleByByteLE(outArr, i * 8 + LEN_BYTES), 0);

        assertArrayEquals(arr, stream.readDoubleArray(), 0);
    }

    /**
     *
     */
    @Test
    public void testCharArrayInternal() {
        char[] arr = new char[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (char)RND.nextInt();

        testWriteArrayInternal(arr, false, false, 1);
        testWriteArrayInternal(arr, false, true, 1);
        testWriteArrayInternal(arr, true, false, 1);
        testWriteArrayInternal(arr, true, true, 1);

        testWriteArrayInternalOverflow(arr, false, false, 1);
        testWriteArrayInternalOverflow(arr, false, true, 1);
        testWriteArrayInternalOverflow(arr, true, false, 1);
        testWriteArrayInternalOverflow(arr, true, true, 1);
    }

    /**
     *
     */
    @Test
    public void testShortArrayInternal() {
        short[] arr = new short[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (short) RND.nextInt();

        testWriteArrayInternal(arr, false, false, 1);
        testWriteArrayInternal(arr, false, true, 1);
        testWriteArrayInternal(arr, true, false, 1);
        testWriteArrayInternal(arr, true, true, 1);

        testWriteArrayInternalOverflow(arr, false, false, 1);
        testWriteArrayInternalOverflow(arr, false, true, 1);
        testWriteArrayInternalOverflow(arr, true, false, 1);
        testWriteArrayInternalOverflow(arr, true, true, 1);
    }

    /**
     *
     */
    @Test
    public void testIntArrayInternal() {
        int[] arr = new int[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextInt();

        testWriteArrayInternal(arr, false, false,2);
        testWriteArrayInternal(arr, false, true,2);
        testWriteArrayInternal(arr, true, false,2);
        testWriteArrayInternal(arr, true, true,2);

        testWriteArrayInternalOverflow(arr, false, false,2);
        testWriteArrayInternalOverflow(arr, false, true,2);
        testWriteArrayInternalOverflow(arr, true, false,2);
        testWriteArrayInternalOverflow(arr, true, true,2);
    }

    /**
     *
     */
    @Test
    public void testLongArrayInternal() {
        long[] arr = new long[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextLong();

        testWriteArrayInternal(arr, false, false, 3);
        testWriteArrayInternal(arr, false, true, 3);
        testWriteArrayInternal(arr, true, false, 3);
        testWriteArrayInternal(arr, true, true, 3);

        testWriteArrayInternalOverflow(arr, false, false, 3);
        testWriteArrayInternalOverflow(arr, false, true, 3);
        testWriteArrayInternalOverflow(arr, true, false, 3);
        testWriteArrayInternalOverflow(arr, true, true, 3);
    }

    /**
     *
     */
    @Test
    public void testFloatArrayInternal() {
        float[] arr = new float[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextFloat();

        testWriteArrayInternal(arr, false, false, 2);
        testWriteArrayInternal(arr, false, true, 2);
        testWriteArrayInternal(arr, true, false, 2);
        testWriteArrayInternal(arr, true, true, 2);

        testWriteArrayInternalOverflow(arr, false, false, 2);
        testWriteArrayInternalOverflow(arr, false, true, 2);
        testWriteArrayInternalOverflow(arr, true, false, 2);
        testWriteArrayInternalOverflow(arr, true, true, 2);
    }

    /**
     *
     */
    @Test
    public void testDoubleArrayInternal() {
        double[] arr = new double[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextDouble();

        testWriteArrayInternal(arr, false, false, 3);
        testWriteArrayInternal(arr, false, true, 3);
        testWriteArrayInternal(arr, true, false, 3);
        testWriteArrayInternal(arr, true, true, 3);

        testWriteArrayInternalOverflow(arr, false, false, 3);
        testWriteArrayInternalOverflow(arr, false, true, 3);
        testWriteArrayInternalOverflow(arr, true, false, 3);
        testWriteArrayInternalOverflow(arr, true, true, 3);
    }

    /**
     * @param srcArr Source array.
     * @param writeBigEndian If {@code true}, then write in big-endian mode.
     * @param readBigEndian If {@code true}, then read in big-endian mode.
     * @param lenShift Length shift.
     * @param <T> Array type.
     */
    private <T> void testWriteArrayInternal(T srcArr, boolean writeBigEndian, boolean readBigEndian, int lenShift) {
        DirectByteBufferStreamImplV2 writeStream = createStream(buff);
        DirectByteBufferStreamImplV2 readStream = createStream(buff);

        int outBytes = (ARR_LEN << lenShift) + LEN_BYTES;
        int typeSize = 1 << lenShift;
        long baseOff = baseOffset(srcArr);

        buff.limit(outBytes);
        buff.rewind();

        boolean writeRes;

        if (writeBigEndian)
            writeRes = writeStream.writeArrayLE(srcArr, baseOff, ARR_LEN, typeSize, lenShift);
        else
            writeRes = writeStream.writeArray(srcArr, baseOff, ARR_LEN, ARR_LEN << lenShift);

        assertTrue(writeRes);

        buff.rewind();

        DirectByteBufferStreamImplV2.ArrayCreator<T> arrCreator = arrayCreator(srcArr);

        T resArr;

        if (readBigEndian)
            resArr = readStream.readArrayLE(arrCreator, typeSize, lenShift, baseOff);
        else
            resArr = readStream.readArray(arrCreator, lenShift, baseOff);

        assertNotNull(resArr);

        if (readBigEndian != writeBigEndian)
            revertByteOrder(resArr, baseOff, typeSize);

        assertEquals(toList(srcArr), toList(resArr));
    }

    /**
     * @param srcArr Source array.
     * @param writeBigEndian If {@code true}, then write in big-endian mode.
     * @param readBigEndian If {@code true}, then read in big-endian mode.
     * @param lenShift Length shift.
     * @param <T> Array type.
     */
    private <T> void testWriteArrayInternalOverflow(T srcArr, boolean writeBigEndian, boolean readBigEndian, int lenShift) {
        DirectByteBufferStreamImplV2 writeStream = createStream(buff);
        DirectByteBufferStreamImplV2 readStream = createStream(buff);

        int outBytes = (ARR_LEN << lenShift) + LEN_BYTES;
        int typeSize = 1 << lenShift;
        long baseOff = baseOffset(srcArr);

        buff.limit(outBytes - 1);
        buff.rewind();

        // Write and read the first part.
        boolean writeRes;

        if (writeBigEndian)
            writeRes = writeStream.writeArrayLE(srcArr, baseOff, ARR_LEN, typeSize, lenShift);
        else
            writeRes = writeStream.writeArray(srcArr, baseOff, ARR_LEN, ARR_LEN << lenShift);

        assertFalse(writeRes);

        buff.limit(buff.position());
        buff.rewind();

        DirectByteBufferStreamImplV2.ArrayCreator<T> arrCreator = arrayCreator(srcArr);

        T resArr;

        if (readBigEndian)
            resArr = readStream.readArrayLE(arrCreator, typeSize, lenShift, baseOff);
        else
            resArr = readStream.readArray(arrCreator, lenShift, baseOff);

        assertEquals(null, resArr);

        buff.limit(outBytes);
        buff.rewind();

        // Write and read the second part.
        if (writeBigEndian)
            writeRes = writeStream.writeArrayLE(srcArr, baseOff, ARR_LEN, typeSize, lenShift);
        else
            writeRes = writeStream.writeArray(srcArr, baseOff, ARR_LEN, ARR_LEN << lenShift);

        assertTrue(writeRes);

        buff.limit(buff.position());
        buff.rewind();

        if (readBigEndian)
            resArr = readStream.readArrayLE(arrCreator, typeSize, lenShift, baseOff);
        else
            resArr = readStream.readArray(arrCreator, lenShift, baseOff);

        assertNotNull(resArr);

        if (readBigEndian != writeBigEndian)
            revertByteOrder(resArr, baseOff, typeSize);

        assertEquals(toList(srcArr), toList(resArr));
    }

    /**
     * Convert provided array into a list.
     *
     * @param arr Source array.
     * @param <T> Array type.
     * @return {@code List}, containing all elements from the array.
     */
    @SuppressWarnings("unchecked")
    private <T> List toList(T arr) {
        List list = new ArrayList<>();

        int len = Array.getLength(arr);

        for (int i = 0; i < len; i++)
            list.add(Array.get(arr, i));

        return list;
    }

    /**
     * Change byte order for the provided array.
     *
     * @param arr Source array.
     * @param baseOff Base offset for the provided array.
     * @param typeSize Type size.
     * @param <T> Array type.
     */
    private <T> void revertByteOrder(T arr, long baseOff, int typeSize) {
        int len = Array.getLength(arr);

        byte[] tmp = new byte[typeSize];

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < typeSize; j++)
                tmp[j] = GridUnsafe.getByteField(arr, baseOff + i * typeSize + j);

            for (int j = 0; j < typeSize; j++)
                GridUnsafe.putByteField(arr, baseOff + i * typeSize + j, tmp[typeSize - j - 1]);
        }
    }

    /**
     * Construct {@code ArrayCreator} for a type of a provided array.
     *
     * @param arr Reference array.
     * @param <T> Array type.
     * @return {@code ArrayCreator} for a required type.
     */
    private <T> DirectByteBufferStreamImplV2.ArrayCreator<T> arrayCreator(final T arr) {
        return new DirectByteBufferStreamImplV2.ArrayCreator<T>() {
            @Override public T create(int len) {
                if (len < 0)
                    throw new IgniteException("Invalid array length: " + len);
                else
                    return (T)Array.newInstance(arr.getClass().getComponentType(), len);
            }
        };
    }

    /**
     * @param arr Array.
     * @param <T> Array type.
     * @return Base offset for the provided array type.
     */
    private <T> long baseOffset(T arr) {
        if (arr.getClass().getComponentType() == char.class)
            return GridUnsafe.CHAR_ARR_OFF;
        else if (arr.getClass().getComponentType() == short.class)
            return GridUnsafe.SHORT_ARR_OFF;
        else if (arr.getClass().getComponentType() == int.class)
            return GridUnsafe.INT_ARR_OFF;
        else if (arr.getClass().getComponentType() == long.class)
            return GridUnsafe.LONG_ARR_OFF;
        else if (arr.getClass().getComponentType() == float.class)
            return GridUnsafe.FLOAT_ARR_OFF;
        else if (arr.getClass().getComponentType() == double.class)
            return GridUnsafe.DOUBLE_ARR_OFF;
        else
            throw new IllegalArgumentException("Unsupported array type");
    }
}
