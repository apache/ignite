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

package org.apache.ignite.internal.binary.streams;

import org.apache.ignite.internal.binary.BinaryPositionReadable;

/**
 * Binary input stream.
 */
public interface BinaryInputStream extends BinaryStream, BinaryPositionReadable {
    /**
     * Read byte value.
     *
     * @return Byte value.
     */
    public byte readByte();

    /**
     * Read byte array.
     *
     * @param cnt Expected item count.
     * @return Byte array.
     */
    public byte[] readByteArray(int cnt);

    /**
     * Reads {@code cnt} of bytes into byte array.
     *
     * @param arr Expected item count.
     * @param off offset
     * @param cnt number of bytes to read.
     * @return actual length read.
     */
    public int read(byte[] arr, int off, int cnt);

    /**
     * Read boolean value.
     *
     * @return Boolean value.
     */
    public boolean readBoolean();

    /**
     * Read boolean array.
     *
     * @param cnt Expected item count.
     * @return Boolean array.
     */
    public boolean[] readBooleanArray(int cnt);

    /**
     * Read short value.
     *
     * @return Short value.
     */
    public short readShort();

    /**
     * Read short array.
     *
     * @param cnt Expected item count.
     * @return Short array.
     */
    public short[] readShortArray(int cnt);

    /**
     * Read char value.
     *
     * @return Char value.
     */
    public char readChar();

    /**
     * Read char array.
     *
     * @param cnt Expected item count.
     * @return Char array.
     */
    public char[] readCharArray(int cnt);

    /**
     * Read int value.
     *
     * @return Int value.
     */
    public int readInt();

    /**
     * Read int array.
     *
     * @param cnt Expected item count.
     * @return Int array.
     */
    public int[] readIntArray(int cnt);

    /**
     * Read float value.
     *
     * @return Float value.
     */
    public float readFloat();

    /**
     * Read float array.
     *
     * @param cnt Expected item count.
     * @return Float array.
     */
    public float[] readFloatArray(int cnt);

    /**
     * Read long value.
     *
     * @return Long value.
     */
    public long readLong();

    /**
     * Read long array.
     *
     * @param cnt Expected item count.
     * @return Long array.
     */
    public long[] readLongArray(int cnt);

    /**
     * Read double value.
     *
     * @return Double value.
     */
    public double readDouble();

    /**
     * Read double array.
     *
     * @param cnt Expected item count.
     * @return Double array.
     */
    public double[] readDoubleArray(int cnt);

    /**
     * Gets amount of remaining data in bytes.
     *
     * @return Remaining data.
     */
    public int remaining();
}
