/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

/**
 * Portable input stream.
 */
public interface GridPortableInputStream extends GridPortableStream {
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
     * Read data to byte array.
     *
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @return Amount of actual bytes read.
     */
    public int read(byte[] arr, int off, int len);

    /**
     * Read data to the given address.
     *
     * @param addr Address.
     * @param len Length.
     * @return Amount of actual bytes read.
     */
    public int read(long addr, int len);

    /**
     * Gets amount of remaining data in bytes.
     *
     * @return Remaining data.
     */
    public int remaining();
}
