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

package org.apache.ignite.internal.portable.streams;

/**
 * Portable input stream.
 */
public interface PortableInputStream extends PortableStream {
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
     * Read int value at the given position.
     *
     * @param pos Position.
     * @return Value.
     */
    public int readInt(int pos);

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