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
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.util.*;

import static org.gridgain.grid.kernal.portable.GridPortableMarshaller.*;

/**
 * Portable writer adapter.
 */
abstract class GridPortableWriterAdapter implements GridPortableWriter {
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
    private static final int INIT_CAP = 4 * 1024;

//    /** */
//    private final Map<String, > fieldNames = new ArrayList<>();
//
//    private final

    /** */
    protected GridPortableByteArray arr;

    /** */
    protected GridPortableWriterAdapter() {
        arr = new GridPortableByteArray(INIT_CAP);
    }

    /**
     * @param arr Array.
     */
    protected GridPortableWriterAdapter(GridPortableByteArray arr) {
        this.arr = arr;
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        return arr.entireArray();
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    public void reserve(int bytes) {
        arr.requestFreeSize(bytes);
    }

    /**
     * @param obj Object to marshal.
     * @throws GridPortableException In case of error.
     */
    public void marshal(Object obj) throws GridPortableException {
        assert obj != null;

        GridPortableClassDescriptor desc = GridPortableClassDescriptor.get(obj.getClass());

        assert desc != null;

        desc.write(obj, this);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID uuid) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID uuid) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(String fieldName, @Nullable T obj) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(@Nullable T obj) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws GridPortableException {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException {
        // TODO
    }

    /**
     * @param val Byte value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteByte(byte val) throws GridPortableException;

    /**
     * @param val Short value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteShort(short val) throws GridPortableException;

    /**
     * @param val Integer value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteInt(int val) throws GridPortableException;

    /**
     * @param val Long value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteLong(long val) throws GridPortableException;

    /**
     * @param val Float value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteFloat(float val) throws GridPortableException;

    /**
     * @param val Double value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteDouble(double val) throws GridPortableException;

    /**
     * @param val Char value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteChar(char val) throws GridPortableException;

    /**
     * @param val Boolean value.
     * @throws GridPortableException In case of error.
     */
    protected abstract void doWriteBoolean(boolean val) throws GridPortableException;

    /**
     * @param val String value.
     * @throws GridPortableException In case of error.
     */
    private void doWriteString(@Nullable String val) throws GridPortableException {
        doWriteByteArray(val != null ? val.getBytes() : null); // TODO: UTF-8
    }

    /**
     * @param uuid UUID.
     * @throws GridPortableException In case of error.
     */
    private void doWriteUuid(@Nullable UUID uuid) throws GridPortableException {
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
     * @throws GridPortableException In case of error.
     */
    private <T> void doWriteObject(@Nullable T obj) throws GridPortableException {
        if (obj == null)
            doWriteInt(NULL);

        // TODO: Handle.

        GridPortableWriterAdapter writer = new GridUnsafePortableWriter(arr);

        writer.marshal(obj);
    }

    /**
     * @param val Byte array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteByteArray(@Nullable byte[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr.array(),
                BYTE_ARR_OFF + arr.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Short array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteShortArray(@Nullable short[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, SHORT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Integer array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteIntArray(@Nullable int[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, INT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    private void doWriteLongArray(@Nullable long[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, LONG_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Float array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteFloatArray(@Nullable float[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Double array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteDoubleArray(@Nullable double[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Char array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteCharArray(@Nullable char[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, CHAR_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Boolean array.
     * @throws GridPortableException In case of error.
     */
    private void doWriteBooleanArray(@Nullable boolean[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, arr.array(),
                BYTE_ARR_OFF + arr.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Array of strings.
     * @throws GridPortableException In case of error.
     */
    private void doWriteStringArray(@Nullable String[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            for (String str : val)
                writeString(str);
        }
    }

    /**
     * @param val Array of UUIDs.
     * @throws GridPortableException In case of error.
     */
    private void doWriteUuidArray(@Nullable UUID[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            for (UUID uuid : val)
                writeUuid(uuid);
        }
    }

    /**
     * @param val Array of objects.
     * @throws GridPortableException In case of error.
     */
    private void doWriteObjectArray(@Nullable Object[] val) throws GridPortableException {
        writeInt(val != null ? val.length : -1);

        if (val != null) {
            for (Object obj : val)
                writeObject(obj);
        }
    }

    /**
     * @param col Collection.
     * @throws GridPortableException In case of error.
     */
    private <T> void doWriteCollection(@Nullable Collection<T> col) throws GridPortableException {
        writeInt(col != null ? col.size() : -1);

        if (col != null) {
            for (Object obj : col)
                writeObject(obj);
        }
    }

    /**
     * @param map Map.
     * @throws GridPortableException In case of error.
     */
    private  <K, V> void doWriteMap(@Nullable Map<K, V> map) throws GridPortableException {
        writeInt(map != null ? map.size() : -1);

        if (map != null) {
            for (Map.Entry<K, V> e : map.entrySet()) {
                writeObject(e.getKey());
                writeObject(e.getValue());
            }
        }
    }
}
