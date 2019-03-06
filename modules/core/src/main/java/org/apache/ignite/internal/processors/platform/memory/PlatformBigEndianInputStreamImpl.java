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

/**
 * Interop input stream implementation working with BIG ENDIAN architecture.
 */
public class PlatformBigEndianInputStreamImpl extends PlatformInputStreamImpl {
    /**
     * Constructor.
     *
     * @param mem Memory chunk.
     */
    public PlatformBigEndianInputStreamImpl(PlatformMemory mem) {
        super(mem);
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        return Short.reverseBytes(super.readShort());
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        short[] res = super.readShortArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Short.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        return Character.reverseBytes(super.readChar());
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        char[] res = super.readCharArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Character.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        return Integer.reverseBytes(super.readInt());
    }

    /** {@inheritDoc} */
    @Override public byte readBytePositioned(int pos) {
        return super.readBytePositioned(pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        return Short.reverseBytes(super.readShortPositioned(pos));
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        return Integer.reverseBytes(super.readIntPositioned(pos));
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        int[] res = super.readIntArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Integer.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        return Float.intBitsToFloat(Integer.reverseBytes(Float.floatToIntBits(super.readFloat())));
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        float[] res = super.readFloatArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Float.intBitsToFloat(Integer.reverseBytes(Float.floatToIntBits(res[i])));

        return res;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        return Long.reverseBytes(super.readLong());
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        long[] res = super.readLongArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Long.reverseBytes(res[i]);

        return res;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        return Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(super.readDouble())));
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        double[] res = super.readDoubleArray(cnt);

        for (int i = 0; i < cnt; i++)
            res[i] = Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(res[i])));

        return res;
    }
}