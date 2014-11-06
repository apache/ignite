/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.streams;

import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridHadoopDataStreamSelfTest extends GridCommonAbstractTest {

    public void testStreams() throws IOException {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

        GridHadoopDataOutStream out = new GridHadoopDataOutStream(mem);

        int size = 4 * 1024;

        final long ptr = mem.allocate(size);

        out.buffer().set(ptr, size);

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
        out.write(new byte[]{1,2,3});
        out.write(new byte[]{0,1,2,3}, 1, 2);
        out.writeUTF("mom washes rum");

        GridHadoopDataInStream in = new GridHadoopDataInStream(mem);

        in.buffer().set(ptr, out.buffer().pointer());

        assertEquals(false, in.readBoolean());
        assertEquals(true, in.readBoolean());
        assertEquals(false, in.readBoolean());
        assertEquals(17, in.read());
        assertEquals(121, in.read());
        assertEquals(0xfa, in.read());
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

        in.read(b);

        assertTrue(Arrays.equals(new byte[]{1,2,3}, b));

        b = new byte[4];

        in.read(b, 1, 2);

        assertTrue(Arrays.equals(new byte[]{0, 1, 2, 0}, b));

        assertEquals("mom washes rum", in.readUTF());
    }
}
