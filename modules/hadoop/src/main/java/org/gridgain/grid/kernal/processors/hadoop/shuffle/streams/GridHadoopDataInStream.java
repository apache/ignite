/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.streams;

import org.gridgain.grid.util.offheap.unsafe.*;

import java.io.*;
import java.nio.charset.*;

/**
 * Data input stream.
 */
public class GridHadoopDataInStream extends InputStream implements DataInput {
    /** */
    private final GridHadoopOffheapBuffer buf = new GridHadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public GridHadoopDataInStream(GridUnsafeMemory mem) {
        assert mem != null;

        this.mem = mem;
    }

    /**
     * @return Buffer.
     */
    public GridHadoopOffheapBuffer buffer() {
        return buf;
    }

    /**
     * @param size Size.
     * @return Old pointer.
     */
    protected long move(long size) throws IOException {
        long ptr = buf.move(size);

        assert ptr != 0;

        return ptr;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return readUnsignedByte();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        readFully(b, off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) throws IOException {
        move(n);

        return n;
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        mem.readBytes(move(len), b, off, len);
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        move(n);

        return n;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        byte res = readByte();

        if (res == 1)
            return true;

        assert res == 0 : res;

        return false;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        return mem.readByte(move(1));
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        return mem.readShort(move(2));
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        return (char)readShort();
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        return mem.readInt(move(4));
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        return mem.readLong(move(8));
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        return mem.readFloat(move(4));
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        return mem.readDouble(move(8));
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        byte[] bytes = new byte[readInt()];

        if (bytes.length != 0)
            readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
