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

import java.nio.ByteBuffer;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.GridTestIoUtils.getCharByByteLE;
import static org.apache.ignite.GridTestIoUtils.getDoubleByByteLE;
import static org.apache.ignite.GridTestIoUtils.getFloatByByteLE;
import static org.apache.ignite.GridTestIoUtils.getIntByByteLE;
import static org.apache.ignite.GridTestIoUtils.getLongByByteLE;
import static org.apache.ignite.GridTestIoUtils.getShortByByteLE;
import static org.junit.Assert.assertArrayEquals;

/**
 * {@link DirectByteBufferStreamImplV2} byte order sanity tests.
 */
public class DirectByteBufferStreamImplV2ByteOrderSelfTest extends TestCase {
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

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        stream = new DirectByteBufferStreamImplV2(new MessageFactory() {
            @Nullable @Override public Message create(short type) {
                return null;
            }
        });

        outArr = new byte[ARR_LEN * 8 + LEN_BYTES];

        buff = ByteBuffer.wrap(outArr);

        stream.setBuffer(buff);
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testCharArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testIntArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testLongArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testFloatArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testDoubleArray() throws Exception {
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
     * @throws Exception If failed.
     */
    public void testShortArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        short[] arr = new short[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (short)RND.nextLong();

        int typeSize = 2;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeShortArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readShortArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeShortArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readShortArray());

        assertTrue(readStream.lastFinished());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        char[] arr = new char[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = (char)RND.nextLong();

        int typeSize = 2;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeCharArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readCharArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeCharArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readCharArray());

        assertTrue(readStream.lastFinished());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        int[] arr = new int[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextInt();

        int typeSize = 4;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeIntArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readIntArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeIntArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readIntArray());

        assertTrue(readStream.lastFinished());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        long[] arr = new long[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextLong();

        int typeSize = 8;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeLongArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readLongArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeLongArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readLongArray());

        assertTrue(readStream.lastFinished());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        float[] arr = new float[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextFloat();

        int typeSize = 4;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeFloatArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readFloatArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeFloatArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readFloatArray(), 0);

        assertTrue(readStream.lastFinished());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleArrayOverflow() throws Exception {
        DirectByteBufferStream readStream = readStream();

        double[] arr = new double[ARR_LEN];

        for (int i = 0; i < ARR_LEN; i++)
            arr[i] = RND.nextDouble();

        int typeSize = 8;

        int outLen = ARR_LEN * typeSize + LEN_BYTES;

        buff.limit(outLen - 1);

        // Write and read the first part.
        stream.writeDoubleArray(arr);

        assertFalse(stream.lastFinished());

        buff.rewind();

        assertEquals(null, readStream.readDoubleArray());

        assertFalse(readStream.lastFinished());

        buff.rewind();

        // Write and read the second part.
        stream.writeDoubleArray(arr);

        assertTrue(stream.lastFinished());

        buff.rewind();

        assertArrayEquals(arr, readStream.readDoubleArray(), 0);

        assertTrue(readStream.lastFinished());
    }

    /**
     * Create DirectByteBufferStream for reading from {@code buff}.
     *
     * @return read stream.
     */
    private DirectByteBufferStream readStream() {
        DirectByteBufferStream readStream = new DirectByteBufferStreamImplV2(new MessageFactory() {
            @Nullable @Override public Message create(short type) {
                return null;
            }
        });

        readStream.setBuffer(buff);

        return readStream;
    }
}