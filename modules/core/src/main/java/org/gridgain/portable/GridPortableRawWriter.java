/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Writer for portable object.
 */
public interface GridPortableRawWriter {
    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByte(byte val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShort(short val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeInt(int val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLong(long val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloat(float val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDouble(double val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeChar(char val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBoolean(boolean val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeString(@Nullable String val) throws GridPortableException;

    /**
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuid(@Nullable UUID val) throws GridPortableException;

    /**
     * @param val Date to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDate(@Nullable Date val) throws GridPortableException;

    /**
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObject(@Nullable Object obj) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByteArray(@Nullable byte[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShortArray(@Nullable short[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeIntArray(@Nullable int[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLongArray(@Nullable long[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloatArray(@Nullable float[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDoubleArray(@Nullable double[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeCharArray(@Nullable char[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeStringArray(@Nullable String[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDateArray(@Nullable Date[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObjectArray(@Nullable Object[] val) throws GridPortableException;

    /**
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException;
}
