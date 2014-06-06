/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.portable;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * TODO 8491.
 */
public interface GridPortableWriter {
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeBoolean(String fieldName, boolean val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeByte(String fieldName, byte val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeByteArray(String fieldName, @Nullable byte[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeShort(String fieldName, short val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeShortArray(String fieldName, @Nullable short[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeChar(String fieldName, char val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeCharArray(String fieldName, @Nullable char[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeInt(String fieldName, int val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeIntArray(String fieldName, @Nullable int[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeLong(String fieldName, long val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeLongArray(String fieldName, @Nullable long[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeFloat(String fieldName, float val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeFloatArray(String fieldName, @Nullable float[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeDouble(String fieldName, double val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeDoubleArray(String fieldName, double[] val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws IOException In case of error.
     */
    public void writeString(String fieldName, @Nullable String val) throws IOException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws IOException In case of error.
     */
    public <T> void writeObject(String fieldName, @Nullable T obj) throws IOException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws IOException In case of error.
     */
    public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws IOException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws IOException In case of error.
     */
    public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws IOException;

    /**
     * @param fieldName Field name.
     * @param uuid UUID to write.
     * @throws IOException In case of error.
     */
    public void writeUuid(String fieldName, @Nullable UUID uuid)throws IOException;
}
