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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Data input stream.
 */
public class HadoopDataInStream extends InputStream implements DataInput {
    /** */
    private final HadoopOffheapBuffer buf = new HadoopOffheapBuffer(0, 0);

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     */
    public HadoopDataInStream(GridUnsafeMemory mem) {
        assert mem != null;

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
     * @return Old pointer.
     * @throws IOException On error.
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
        if (buf.remaining() == 0)
            return null;

        SB sb = new SB();

        while (buf.remaining() > 0) {
            char c = (char)readByte();

            switch (c) {
                case '\n':
                    return sb.toString();

                case '\r':
                    if (buf.remaining() == 0)
                        return sb.toString();

                    c = (char)readByte();

                    if (c == '\n')
                        return sb.toString();
                    else
                        buf.moveBackward(1);

                    return sb.toString();

                default:
                    sb.a(c);
            }
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        byte[] bytes = new byte[readInt()];

        if (bytes.length != 0)
            readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}