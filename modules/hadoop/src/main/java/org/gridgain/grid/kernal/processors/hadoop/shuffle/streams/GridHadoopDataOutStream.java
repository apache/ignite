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

import static org.gridgain.grid.util.offheap.unsafe.GridUnsafeMemory.*;

/**
 * Data output stream.
 */
public class GridHadoopDataOutStream extends OutputStream implements DataOutput {
    /** */
    private final GridHadoopOffheapBuffer buf = new GridHadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public GridHadoopDataOutStream(GridUnsafeMemory mem) {
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
     * @return Old pointer or {@code 0} if move was impossible.
     */
    public long move(long size) {
        return buf.move(size);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        writeByte(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) {
        write(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) {
        UNSAFE.copyMemory(b, BYTE_ARR_OFF + off, null, move(len), len);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean v) {
        writeByte(v ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) {
        mem.writeByte(move(1), (byte)v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) {
        mem.writeShort(move(2), (short)v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) {
        writeShort(v);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int v) {
        mem.writeInt(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long v) {
        mem.writeLong(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float v) {
        mem.writeFloat(move(4), v);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double v) {
        mem.writeDouble(move(8), v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(String s) {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) {
        writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);

        writeInt(b.length);
        write(b);
    }
}
