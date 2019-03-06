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

package org.apache.ignite.internal.processors.platform.memory;

import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Interop output stream implementation working with BIG ENDIAN architecture.
 */
public class PlatformBigEndianOutputStreamImpl extends PlatformOutputStreamImpl {
    /**
     * Constructor.
     *
     * @param mem Underlying memory chunk.
     */
    public PlatformBigEndianOutputStreamImpl(PlatformMemory mem) {
        super(mem);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        super.writeShort(Short.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (short item : val) {
            GridUnsafe.putShort(startPos, Short.reverseBytes(item));

            startPos += 2;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        super.writeChar(Character.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        int cnt = val.length << 1;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (char item : val) {
            GridUnsafe.putChar(startPos, Character.reverseBytes(item));

            startPos += 2;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        super.writeInt(Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (int item : val) {
            GridUnsafe.putInt(startPos, Integer.reverseBytes(item));

            startPos += 4;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int pos, short val) {
        super.writeShort(pos, Short.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        super.writeInt(pos, Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        int cnt = val.length << 2;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (float item : val) {
            GridUnsafe.putInt(startPos, Integer.reverseBytes(Float.floatToIntBits(item)));

            startPos += 4;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        super.writeLong(Long.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (long item : val) {
            GridUnsafe.putLong(startPos, Long.reverseBytes(item));

            startPos += 8;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        int cnt = val.length << 3;

        ensureCapacity(pos + cnt);

        long startPos = data + pos;

        for (double item : val) {
            GridUnsafe.putLong(startPos, Long.reverseBytes(Double.doubleToLongBits(item)));

            startPos += 8;
        }

        shift(cnt);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(short val) {
        super.unsafeWriteShort(Short.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(int pos, short val) {
        super.unsafeWriteShort(pos, Short.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteChar(char val) {
        super.unsafeWriteChar(Character.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int val) {
        super.unsafeWriteInt(Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int pos, int val) {
        super.unsafeWriteInt(pos, Integer.reverseBytes(val));
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteLong(long val) {
        super.unsafeWriteLong(Long.reverseBytes(val));
    }
}
