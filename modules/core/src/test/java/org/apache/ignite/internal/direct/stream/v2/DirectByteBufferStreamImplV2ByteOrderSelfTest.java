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
            @Nullable @Override public Message create(byte type) {
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
    public void testShort() throws Exception {
        short val = (short)RND.nextLong();

        stream.writeShort(val);

        buff.rewind();

        assertEquals(val, getShortByByteLE(outArr));
        assertEquals(val, stream.readShort());
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
    public void testChar() throws Exception {
        char val = (char)RND.nextLong();

        stream.writeChar(val);

        buff.rewind();

        assertEquals(val, getCharByByteLE(outArr));
        assertEquals(val, stream.readChar());
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
    public void testFloat() throws Exception {
        float val = RND.nextFloat();

        stream.writeFloat(val);

        buff.rewind();

        assertEquals(val, getFloatByByteLE(outArr), 0);
        assertEquals(val, stream.readFloat(), 0);
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
    public void testDouble() throws Exception {
        double val = RND.nextDouble();

        stream.writeDouble(val);

        buff.rewind();

        assertEquals(val, getDoubleByByteLE(outArr), 0);
        assertEquals(val, stream.readDouble(), 0);
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
}