/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

import java.io.DataOutput;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;

/**
 * Data output stream.
 */
public class HadoopDataOutStream extends OutputStream implements DataOutput {
    /** */
    private final HadoopOffheapBuffer buf = new HadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public HadoopDataOutStream(GridUnsafeMemory mem) {
        this.mem = mem;
    }

    /**
     * @return Buffer.
     */
    public HadoopOffheapBuffer buffer() {
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
        GridUnsafe.copyHeapOffheap(b, GridUnsafe.BYTE_ARR_OFF + off, move(len), len);
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