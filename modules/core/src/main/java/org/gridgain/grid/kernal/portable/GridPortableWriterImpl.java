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
    private final GridPortableHeaderWriter hdrWriter = new GridPortableHeaderWriter(false); // TODO: useNames flag

    /** */
    private final Collection<WriteAction> data = new ArrayList<>();

    /** */
    private final Collection<WriteAction> rawData = new ArrayList<>();

    /** */
    protected GridPortableByteArray arr;

    /** */
    protected GridPortableWriterImpl() {
        arr = new GridPortableByteArray(INIT_CAP);
    }

    /**
     * @param writer Writer.
     */
    protected GridPortableWriterImpl(GridPortableWriterImpl writer) {
        arr = writer.arr;
    }

    /**
     * @return Array.
     */
    byte[] array() {
        return arr.entireArray();
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    void reserve(int bytes) {
        arr.requestFreeSize(bytes);
    }

    /**
     * @throws GridPortableException In case of error.
     */
    void flush() throws GridPortableException {
        doWriteByteArray(hdrWriter.header()); // TODO: Write directly to writer

        for (WriteAction act : data)
            act.apply();

        writeCurrentSize(RAW_DATA_OFF_POS);

        for (WriteAction act : rawData)
            act.apply();
    }

    /**
     * @param off Offset.
     */
    void writeCurrentSize(int off) {
        PRIM.writeInt(arr.array(), off, arr.size());
    }

    /**
     * @param val Byte array.
     */
    void write(byte[] val) {
        assert val != null;

        UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Value.
     */
    void doWriteByte(byte val) {
        PRIM.writeByte(arr.array(), arr.requestFreeSize(1), val);
    }

    /**
     * @param val Value.
     */
    void doWriteShort(short val) {
        PRIM.writeShort(arr.array(), arr.requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteInt(int val) {
        PRIM.writeInt(arr.array(), arr.requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteLong(long val) {
        PRIM.writeLong(arr.array(), arr.requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteFloat(float val) {
        PRIM.writeFloat(arr.array(), arr.requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteDouble(double val) {
        PRIM.writeDouble(arr.array(), arr.requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteChar(char val) {
        PRIM.writeChar(arr.array(), arr.requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteBoolean(boolean val) {
        PRIM.writeBoolean(arr.array(), arr.requestFreeSize(1), val);
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
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr.array(),
                BYTE_ARR_OFF + arr.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, SHORT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, INT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
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

            UNSAFE.copyMemory(val, LONG_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, CHAR_ARR_OFF, arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, arr.array(),
                BYTE_ARR_OFF + arr.requestFreeSize(val.length), val.length);
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
    @Override public void writeByte(String fieldName, final byte val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteByte(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeByte(final byte val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteByte(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, final short val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteShort(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeShort(final short val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteShort(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, final int val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteInt(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeInt(final int val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteInt(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, final long val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteLong(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeLong(final long val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteLong(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, final float val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteFloat(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(final float val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteFloat(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, final double val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteDouble(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(final double val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteDouble(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, final char val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteChar(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeChar(final char val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteChar(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, final boolean val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteBoolean(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(final boolean val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteBoolean(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable final String val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteString(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable final String val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteString(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable final UUID val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteUuid(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable final UUID val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteUuid(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(String fieldName, @Nullable final T obj) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                writeCurrentSize(offPos);

                doWriteObject(obj);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObject(@Nullable final T obj) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                doWriteObject(obj);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable final byte[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteByteArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable final byte[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteByteArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable final short[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteShortArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable final short[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteShortArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable final int[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteIntArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable final int[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteIntArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable final long[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteLongArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable final long[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteLongArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable final float[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteFloatArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable final float[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteFloatArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable final double[] val)
        throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteDoubleArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable final double[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteDoubleArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable final char[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteCharArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable final char[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteCharArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable final boolean[] val)
        throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteBooleanArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable final boolean[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteBooleanArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable final String[] val)
        throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteStringArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable final String[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteStringArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable final UUID[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() {
                writeCurrentSize(offPos);

                doWriteUuidArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable final UUID[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() {
                doWriteUuidArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable final Object[] val) throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                writeCurrentSize(offPos);

                doWriteObjectArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable final Object[] val) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                doWriteObjectArray(val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable final Collection<T> col)
        throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                writeCurrentSize(offPos);

                doWriteCollection(col);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable final Collection<T> col) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                doWriteCollection(col);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable final Map<K, V> map)
        throws GridPortableException {
        final int offPos = hdrWriter.addField(fieldName);

        data.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                writeCurrentSize(offPos);

                doWriteMap(map);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable final Map<K, V> map) throws GridPortableException {
        rawData.add(new WriteAction() {
            @Override public void apply() throws GridPortableException {
                doWriteMap(map);
            }
        });
    }

    /** */
    private static interface WriteAction {
        /**
         * @throws GridPortableException In case of error.
         */
        public void apply() throws GridPortableException;
    }
}
