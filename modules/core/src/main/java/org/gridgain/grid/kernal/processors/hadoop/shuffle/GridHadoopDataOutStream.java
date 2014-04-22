/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.util.offheap.unsafe.*;

import java.io.*;
import java.nio.charset.*;

/**
 * Data output stream.
 */
public class GridHadoopDataOutStream extends OutputStream implements DataOutput {
    /** */
    private final GridHadoopBuffer buf;

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     * @param bufPtr Pointer to the buffer.
     * @param size Buffer size.
     */
    public GridHadoopDataOutStream(GridUnsafeMemory mem, long bufPtr, long size) {
        this.mem = mem;

        buf = new GridHadoopBuffer(bufPtr, size);
    }

    /**
     * @return Buffer.
     */
    public GridHadoopBuffer buffer() {
        return buf;
    }

    /**
     * @param size Size.
     * @return Old pointer.
     */
    protected long move(long size) throws IOException {
        return buf.move(size);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        mem.writeByte(move(1), (byte)b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) throws IOException {
        mem.writeBytes(move(len), b, off, len);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        mem.writeByte(move(1), (byte)v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        mem.writeShort(move(2), (short)v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        writeShort(v);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int v) throws IOException {
        mem.writeInt(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long v) throws IOException {
        mem.writeLong(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float v) throws IOException {
        mem.writeFloat(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double v) throws IOException {
        mem.writeDouble(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(String s) throws IOException {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) throws IOException {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) throws IOException {
        if (s == null)
            writeInt(-1);
        else {
            writeInt(s.length());
            write(s.getBytes(StandardCharsets.UTF_8));
        }
    }
}
