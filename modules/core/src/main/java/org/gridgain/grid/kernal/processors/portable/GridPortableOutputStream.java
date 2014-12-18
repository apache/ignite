/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

/**
 * Portable output stream.
 */
public interface GridPortableOutputStream extends GridPortableStream, AutoCloseable {
    /**
     * Write byte value.
     *
     * @param val Byte value.
     */
    public void writeByte(byte val);

    /**
     * Write byte array.
     *
     * @param val Byte array.
     */
    public void writeByteArray(byte[] val);

    /**
     * Write boolean value.
     *
     * @param val Boolean value.
     */
    public void writeBoolean(boolean val);

    /**
     * Write boolean array.
     *
     * @param val Boolean array.
     */
    public void writeBooleanArray(boolean[] val);

    /**
     * Write short value.
     *
     * @param val Short value.
     */
    public void writeShort(short val);

    /**
     * Write short array.
     *
     * @param val Short array.
     */
    public void writeShortArray(short[] val);

    /**
     * Write char value.
     *
     * @param val Char value.
     */
    public void writeChar(char val);

    /**
     * Write char array.
     *
     * @param val Char array.
     */
    public void writeCharArray(char[] val);

    /**
     * Write int value.
     *
     * @param val Int value.
     */
    public void writeInt(int val);

    /**
     * Write int value to the given position.
     *
     * @param pos Position.
     * @param val Value.
     */
    public void writeInt(int pos, int val);

    /**
     * Write int array.
     *
     * @param val Int array.
     */
    public void writeIntArray(int[] val);

    /**
     * Write float value.
     *
     * @param val Float value.
     */
    public void writeFloat(float val);

    /**
     * Write float array.
     *
     * @param val Float array.
     */
    public void writeFloatArray(float[] val);

    /**
     * Write long value.
     *
     * @param val Long value.
     */
    public void writeLong(long val);

    /**
     * Write long array.
     *
     * @param val Long array.
     */
    public void writeLongArray(long[] val);

    /**
     * Write double value.
     *
     * @param val Double value.
     */
    public void writeDouble(double val);

    /**
     * Write double array.
     *
     * @param val Double array.
     */
    public void writeDoubleArray(double[] val);

    /**
     * Write byte array.
     *
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     */
    public void write(byte[] arr, int off, int len);

    /**
     * Write data from unmanaged memory.
     *
     * @param addr Address.
     * @param cnt Count.
     */
    public void write(long addr, int cnt);

    /**
     * Close the stream releasing resources.
     */
    @Override public void close();
}
