/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.nio.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.kernal.portable.GridPortableMarshaller.*;

/**
 * Portable writer implementation.
 */
class GridPortableWriterImpl implements GridPortableWriter {
    /** */
    protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    protected static final long SHORT_ARR_OFF = UNSAFE.arrayBaseOffset(short[].class);

    /** */
    protected static final long INT_ARR_OFF = UNSAFE.arrayBaseOffset(int[].class);

    /** */
    protected static final long LONG_ARR_OFF = UNSAFE.arrayBaseOffset(long[].class);

    /** */
    protected static final long FLOAT_ARR_OFF = UNSAFE.arrayBaseOffset(float[].class);

    /** */
    protected static final long DOUBLE_ARR_OFF = UNSAFE.arrayBaseOffset(double[].class);

    /** */
    protected static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** */
    protected static final long BOOLEAN_ARR_OFF = UNSAFE.arrayBaseOffset(boolean[].class);

    /** */
    private static final int RAW_DATA_OFF_POS = 14;

    /** */
    private static final int INIT_CAP = 4 * 1024;

    /** */
    private static final GridPortablePrimitives PRIM = GridPortablePrimitives.get();

    /** */
    private static final boolean useNames = false; // TODO: take from config

    /** */
    private byte[] arr;

    /** */
    private int size;

    /** */
    private int mark;

    /** */
    private boolean allowFields = true;

    /** */
    GridPortableWriterImpl() {
        arr = new byte[INIT_CAP];
    }

    /**
     * @return Array.
     */
    ByteBuffer buffer() {
        return ByteBuffer.wrap(arr, 0, size);
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    int reserve(int bytes) {
        int size0 = size;

        requestFreeSize(bytes);

        return size0;
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    int reserveAndMark(int bytes) {
        int size0 = reserve(bytes);

        mark = size;

        return size0;
    }

    /**
     * @param off Offset.
     */
    void writeDelta(int off) {
        PRIM.writeInt(arr, off, size - mark);
    }

    /**
     * @param off Offset.
     */
    void writeCurrentSize(int off) {
        PRIM.writeInt(arr, off, size);
    }

    /**
     * @param val Byte array.
     */
    void write(byte[] val) {
        assert val != null;

        UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Value.
     */
    void doWriteByte(byte val) {
        PRIM.writeByte(arr, requestFreeSize(1), val);
    }

    /**
     * @param val Value.
     */
    void doWriteShort(short val) {
        PRIM.writeShort(arr, requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteInt(int val) {
        PRIM.writeInt(arr, requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteLong(long val) {
        PRIM.writeLong(arr, requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteFloat(float val) {
        PRIM.writeFloat(arr, requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteDouble(double val) {
        PRIM.writeDouble(arr, requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteChar(char val) {
        PRIM.writeChar(arr, requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteBoolean(boolean val) {
        PRIM.writeBoolean(arr, requestFreeSize(1), val);
    }

    /**
     * @param val String value.
     */
    void doWriteString(@Nullable String val) {
        doWriteByteArray(val != null ? val.getBytes(UTF_8) : null);
    }

     /**
     * @param uuid UUID.
     */
    void doWriteUuid(@Nullable UUID uuid) {
        if (uuid == null)
            doWriteBoolean(false);
        else {
            doWriteBoolean(true);
            doWriteLong(uuid.getMostSignificantBits());
            doWriteLong(uuid.getLeastSignificantBits());
        }
    }

    /**
     * @param obj Object.
     */
    <T> void doWriteObject(@Nullable T obj) throws GridPortableException {
        if (obj == null) {
            doWriteInt(NULL);

            return;
        }

        // TODO: Handle.

        doWriteByte(OBJ);

        GridPortableClassDescriptor desc = GridPortableClassDescriptor.get(obj.getClass());

        assert desc != null;

        desc.write(obj, this);
    }

    /**
     * @param val Byte array.
     */
    void doWriteByteArray(@Nullable byte[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, SHORT_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, INT_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Long array.
     * @throws GridPortableException In case of error.
     */
    void doWriteLongArray(@Nullable long[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, LONG_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, CHAR_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, arr, BYTE_ARR_OFF + requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Array of strings.
     */
    void doWriteStringArray(@Nullable String[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            for (String str : val)
                doWriteString(str);
        }
    }

    /**
     * @param val Array of UUIDs.
     */
    void doWriteUuidArray(@Nullable UUID[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            for (UUID uuid : val)
                doWriteUuid(uuid);
        }
    }

    /**
     * @param val Array of objects.
     * @throws GridPortableException In case of error.
     */
    void doWriteObjectArray(@Nullable Object[] val) throws GridPortableException {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            for (Object obj : val)
                doWriteObject(obj);
        }
    }

    /**
     * @param col Collection.
     * @throws GridPortableException In case of error.
     */
    <T> void doWriteCollection(@Nullable Collection<T> col) throws GridPortableException {
        doWriteInt(col != null ? col.size() : -1);

        if (col != null) {
            for (Object obj : col)
                doWriteObject(obj);
        }
    }

    /**
     * @param map Map.
     * @throws GridPortableException In case of error.
     */
    <K, V> void doWriteMap(@Nullable Map<K, V> map) throws GridPortableException {
        doWriteInt(map != null ? map.size() : -1);

        if (map != null) {
            for (Map.Entry<K, V> e : map.entrySet()) {
                doWriteObject(e.getKey());
                doWriteObject(e.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(1);
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws GridPortableException {
        switchToRaw();

        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(2);
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws GridPortableException {
        switchToRaw();

        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(4);
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws GridPortableException {
        switchToRaw();

        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(8);
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws GridPortableException {
        switchToRaw();

        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(4);
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws GridPortableException {
        switchToRaw();

        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(8);
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws GridPortableException {
        switchToRaw();

        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(2);
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws GridPortableException {
        switchToRaw();

        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(1);
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws GridPortableException {
        switchToRaw();

        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws GridPortableException {
        writeFieldName(fieldName);

        byte[] arr = null;
        int len = 4;

        if (val != null) {
            arr = val.getBytes(UTF_8);
            len += arr.length;
        }

        doWriteInt(len);
        doWriteByteArray(arr);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws GridPortableException {
        switchToRaw();

        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 17 : 1);
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws GridPortableException {
        switchToRaw();

        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(String fieldName, @Nullable T obj) throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteObject(obj);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(@Nullable T obj) throws GridPortableException {
        switchToRaw();

        doWriteObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length : 4);
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws GridPortableException {
        switchToRaw();

        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 1 : 4);
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws GridPortableException {
        switchToRaw();

        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 2 : 4);
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws GridPortableException {
        switchToRaw();

        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 3 : 4);
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws GridPortableException {
        switchToRaw();

        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 2 : 4);
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws GridPortableException {
        switchToRaw();

        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 3 : 4);
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws GridPortableException {
        switchToRaw();

        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length << 1 : 4);
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws GridPortableException {
        switchToRaw();

        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws GridPortableException {
        writeFieldName(fieldName);

        doWriteInt(val != null ? 4 + val.length : 4);
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException {
        switchToRaw();

        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteStringArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws GridPortableException {
        switchToRaw();

        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteUuidArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException {
        switchToRaw();

        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteObjectArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws GridPortableException {
        switchToRaw();

        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteCollection(col);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException {
        switchToRaw();

        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws GridPortableException {
        writeFieldName(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteMap(map);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException {
        switchToRaw();

        doWriteMap(map);
    }

    /**
     * @param bytes Number of bytes that are going to be written.
     * @return Offset before write.
     */
    private int requestFreeSize(int bytes) {
        int size0 = size;

        size += bytes;

        if (size > arr.length) {
            byte[] arr0 = new byte[size << 1];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, size0);

            arr = arr0;
        }

        return size0;
    }

    /**
     * @throws GridPortableException If fields are not allowed.
     * @param fieldName
     */
    private void writeFieldName(String fieldName) throws GridPortableException {
        A.notNull(fieldName, "fieldName");

        if (!allowFields)
            throw new GridPortableException("Fields are not allowed."); // TODO: proper message

        if (useNames)
            doWriteByteArray(fieldName.getBytes(UTF_8));
        else
            doWriteInt(fieldName.hashCode());
    }

    /**
     * Restricts fields.
     */
    private void switchToRaw() {
        if (allowFields) {
            writeCurrentSize(RAW_DATA_OFF_POS);

            allowFields = false;
        }
    }
}
