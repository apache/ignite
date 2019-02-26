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

package org.apache.ignite.internal.processors.hadoop.impl.shuffle.streams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;
import org.apache.ignite.internal.processors.hadoop.shuffle.direct.HadoopDirectDataInput;
import org.apache.ignite.internal.processors.hadoop.shuffle.direct.HadoopDirectDataOutput;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataInStream;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataOutStream;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class HadoopDataStreamSelfTest extends GridCommonAbstractTest {
    private static final int BUFF_SIZE = 4 * 1024;

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testStreams() throws IOException {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

        HadoopDataOutStream out = new HadoopDataOutStream(mem);

        final long ptr = mem.allocate(BUFF_SIZE);

        out.buffer().set(ptr, BUFF_SIZE);

        write(out);

        HadoopDataInStream in = new HadoopDataInStream(mem);

        in.buffer().set(ptr, out.buffer().pointer() - ptr);

        checkRead(in);
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testDirectStreams() throws IOException {
        HadoopDirectDataOutput out = new HadoopDirectDataOutput(BUFF_SIZE);

        write(out);

        byte [] inBuf = Arrays.copyOf(out.buffer(), out.position());

        HadoopDirectDataInput in = new HadoopDirectDataInput(inBuf);

        checkRead(in);
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testReadline() throws IOException {
        checkReadLine("String1\rString2\r\nString3\nString4");
        checkReadLine("String1\rString2\r\nString3\nString4\r\n");
        checkReadLine("String1\rString2\r\nString3\nString4\r");
        checkReadLine("\nA\rB\r\nC\nD\n");
        checkReadLine("\rA\rB\r\nC\nD\n");
        checkReadLine("\r\nA\rB\r\nC\nD\n");
        checkReadLine("\r\r\nA\r\r\nC\nD\n");
        checkReadLine("\r\r\r\n\n\n");
        checkReadLine("\r\n");
        checkReadLine("\r");
        checkReadLine("\n");
    }

    /**
     * @param val String value.
     * @throws IOException On error.
     */
    private void checkReadLine(String val) throws IOException {
        List<String> expected = readLineByDataInputStream(val);
        List<String> dataInp = readLineByHadoopDataInStream(val);
        List<String> directDataInp = readLineByHadoopDirectDataInput(val);

        assertEquals(expected, dataInp);
        assertEquals(expected, directDataInp);
    }

    /**
     * @param val String value.
     * @return List of strings are returned by readLine().
     * @throws IOException On error.
     */
    List<String> readLineByDataInputStream(String val) throws IOException {
        ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();

        byteArrayOs.write(val.getBytes());

        byteArrayOs.close();

        try (DataInputStream in =  new DataInputStream(new ByteArrayInputStream(byteArrayOs.toByteArray()))) {
            return readLineStrings(in);
        }
    }

    /**
     * @param val String value.
     * @return List of strings are returned by readLine().
     * @throws IOException On error.
     */
    List<String> readLineByHadoopDataInStream(String val) throws IOException {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

        HadoopDataOutStream out = new HadoopDataOutStream(mem);

        final long ptr = mem.allocate(BUFF_SIZE);

        out.buffer().set(ptr, BUFF_SIZE);

        out.write(val.getBytes());

        HadoopDataInStream in = new HadoopDataInStream(mem);

        in.buffer().set(ptr, out.buffer().pointer() - ptr);

        return readLineStrings(in);
    }

    /**
     * @param val String value.
     * @return List of strings are returned by readLine().
     * @throws IOException On error.
     */
    List<String> readLineByHadoopDirectDataInput(String val) throws IOException {

        HadoopDirectDataOutput out = new HadoopDirectDataOutput(BUFF_SIZE);

        out.write(val.getBytes());

        byte [] inBuf = Arrays.copyOf(out.buffer(), out.position());

        HadoopDirectDataInput in = new HadoopDirectDataInput(inBuf);

        return readLineStrings(in);
    }

    /**
     * @param in Data input.
     * @return List of strings are returned by readLine().
     * @throws IOException On error.
     */
    @NotNull private List<String> readLineStrings(DataInput in) throws IOException {
        List<String> strs = new ArrayList<>();

        for (String str = in.readLine(); str != null; str = in.readLine())
            strs.add(str);

        return strs;
    }

    /**
     * @param out Data output.
     * @throws IOException On error.
     */
    private void write(DataOutput out) throws IOException {
        out.writeBoolean(false);
        out.writeBoolean(true);
        out.writeBoolean(false);
        out.write(17);
        out.write(121);
        out.write(0xfafa);
        out.writeByte(17);
        out.writeByte(121);
        out.writeByte(0xfafa);
        out.writeChar('z');
        out.writeChar('o');
        out.writeChar('r');
        out.writeShort(100);
        out.writeShort(Short.MIN_VALUE);
        out.writeShort(Short.MAX_VALUE);
        out.writeShort(65535);
        out.writeShort(65536); // 0
        out.writeInt(Integer.MAX_VALUE);
        out.writeInt(Integer.MIN_VALUE);
        out.writeInt(-1);
        out.writeInt(0);
        out.writeInt(1);
        out.writeFloat(0.33f);
        out.writeFloat(0.5f);
        out.writeFloat(-0.7f);
        out.writeFloat(Float.MAX_VALUE);
        out.writeFloat(Float.MIN_VALUE);
        out.writeFloat(Float.MIN_NORMAL);
        out.writeFloat(Float.POSITIVE_INFINITY);
        out.writeFloat(Float.NEGATIVE_INFINITY);
        out.writeFloat(Float.NaN);
        out.writeDouble(-12312312.3333333336666779);
        out.writeDouble(123123.234);
        out.writeDouble(Double.MAX_VALUE);
        out.writeDouble(Double.MIN_VALUE);
        out.writeDouble(Double.MIN_NORMAL);
        out.writeDouble(Double.NEGATIVE_INFINITY);
        out.writeDouble(Double.POSITIVE_INFINITY);
        out.writeDouble(Double.NaN);
        out.writeLong(Long.MAX_VALUE);
        out.writeLong(Long.MIN_VALUE);
        out.writeLong(0);
        out.writeLong(-1L);
        out.write(new byte[] {1, 2, 3});
        out.write(new byte[] {0, 1, 2, 3}, 1, 2);
        out.writeUTF("mom washes rum");
    }

    /**
     * @param in Data input.
     * @throws IOException On error.
     */
    private void checkRead(DataInput in) throws IOException {
        assertEquals(false, in.readBoolean());
        assertEquals(true, in.readBoolean());
        assertEquals(false, in.readBoolean());
        assertEquals(17, in.readUnsignedByte());
        assertEquals(121, in.readUnsignedByte());
        assertEquals(0xfa, in.readUnsignedByte());
        assertEquals(17, in.readByte());
        assertEquals(121, in.readByte());
        assertEquals((byte)0xfa, in.readByte());
        assertEquals('z', in.readChar());
        assertEquals('o', in.readChar());
        assertEquals('r', in.readChar());
        assertEquals(100, in.readShort());
        assertEquals(Short.MIN_VALUE, in.readShort());
        assertEquals(Short.MAX_VALUE, in.readShort());
        assertEquals(-1, in.readShort());
        assertEquals(0, in.readShort());
        assertEquals(Integer.MAX_VALUE, in.readInt());
        assertEquals(Integer.MIN_VALUE, in.readInt());
        assertEquals(-1, in.readInt());
        assertEquals(0, in.readInt());
        assertEquals(1, in.readInt());
        assertEquals(0.33f, in.readFloat());
        assertEquals(0.5f, in.readFloat());
        assertEquals(-0.7f, in.readFloat());
        assertEquals(Float.MAX_VALUE, in.readFloat());
        assertEquals(Float.MIN_VALUE, in.readFloat());
        assertEquals(Float.MIN_NORMAL, in.readFloat());
        assertEquals(Float.POSITIVE_INFINITY, in.readFloat());
        assertEquals(Float.NEGATIVE_INFINITY, in.readFloat());
        assertEquals(Float.NaN, in.readFloat());
        assertEquals(-12312312.3333333336666779, in.readDouble());
        assertEquals(123123.234, in.readDouble());
        assertEquals(Double.MAX_VALUE, in.readDouble());
        assertEquals(Double.MIN_VALUE, in.readDouble());
        assertEquals(Double.MIN_NORMAL, in.readDouble());
        assertEquals(Double.NEGATIVE_INFINITY, in.readDouble());
        assertEquals(Double.POSITIVE_INFINITY, in.readDouble());
        assertEquals(Double.NaN, in.readDouble());
        assertEquals(Long.MAX_VALUE, in.readLong());
        assertEquals(Long.MIN_VALUE, in.readLong());
        assertEquals(0, in.readLong());
        assertEquals(-1, in.readLong());

        byte[] b = new byte[3];

        in.readFully(b);

        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, b));

        b = new byte[4];

        in.readFully(b, 1, 2);

        assertTrue(Arrays.equals(new byte[] {0, 1, 2, 0}, b));

        assertEquals("mom washes rum", in.readUTF());
    }
}
