/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.nio.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.kernal.processors.portable.GridPortableMarshaller.*;

/**
 * Portable writer implementation.
 */
class GridPortableWriterImpl implements GridPortableWriter, GridPortableRawWriter {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private static final long SHORT_ARR_OFF = UNSAFE.arrayBaseOffset(short[].class);

    /** */
    private static final long INT_ARR_OFF = UNSAFE.arrayBaseOffset(int[].class);

    /** */
    private static final long LONG_ARR_OFF = UNSAFE.arrayBaseOffset(long[].class);

    /** */
    private static final long FLOAT_ARR_OFF = UNSAFE.arrayBaseOffset(float[].class);

    /** */
    private static final long DOUBLE_ARR_OFF = UNSAFE.arrayBaseOffset(double[].class);

    /** */
    private static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** */
    private static final long BOOLEAN_ARR_OFF = UNSAFE.arrayBaseOffset(boolean[].class);

    /** */
    private static final GridPortablePrimitives PRIM = GridPortablePrimitives.get();

    /** */
    private static final int TOTAL_LEN_POS = 10;

    /** */
    private static final int RAW_DATA_OFF_POS = 14;

    /** */
    private static final int INIT_CAP = 4 * 1024;

    /** */
    private final GridPortableContext ctx;

    /** */
    private final WriterContext wCtx;

    /** */
    private final int start;

    /** */
    private int mark;

    /** */
    private int typeId;

    /** */
    private boolean allowFields = true;

    /** */
    GridPortableWriterImpl(GridPortableContext ctx) {
        this.ctx = ctx;

        wCtx = new WriterContext();

        start = 0;
    }

    /**
     * @param wCtx Context.
     */
    private GridPortableWriterImpl(GridPortableContext ctx, WriterContext wCtx) {
        this.ctx = ctx;
        this.wCtx = wCtx;

        start = wCtx.off;
    }

    /**
     * @param obj Object.
     * @throws GridPortableException In case of error.
     */
    void marshal(Object obj) throws GridPortableException {
        assert obj != null;

        doWriteByte(OBJ);

        GridPortableClassDescriptor desc = ctx.descriptorForClass(obj.getClass());

        if (desc == null)
            throw new GridPortableException("Object is not portable: " + obj);

        typeId = desc.typeId();

        desc.write(obj, this);
    }

    /**
     * @return Array.
     */
    ByteBuffer buffer() {
        return ByteBuffer.wrap(wCtx.arr, 0, wCtx.off);
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    int reserve(int bytes) {
        return wCtx.requestFreeSize(bytes);
    }

    /**
     * @param bytes Number of bytes to reserve.
     */
    int reserveAndMark(int bytes) {
        int off0 = reserve(bytes);

        mark = wCtx.off;

        return off0;
    }

    /**
     * @param off Offset.
     */
    void writeDelta(int off) {
        PRIM.writeInt(wCtx.arr, off, wCtx.off - mark);
    }

    /**
     */
    void writeLength() {
        PRIM.writeInt(wCtx.arr, start + TOTAL_LEN_POS, wCtx.off - start);
    }

    /**
     * @param val Byte array.
     */
    void write(byte[] val) {
        assert val != null;

        UNSAFE.copyMemory(val, BYTE_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Value.
     */
    void doWriteByte(byte val) {
        PRIM.writeByte(wCtx.arr, wCtx.requestFreeSize(1), val);
    }

    /**
     * @param val Value.
     */
    void doWriteShort(short val) {
        PRIM.writeShort(wCtx.arr, wCtx.requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteInt(int val) {
        PRIM.writeInt(wCtx.arr, wCtx.requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteLong(long val) {
        PRIM.writeLong(wCtx.arr, wCtx.requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteFloat(float val) {
        PRIM.writeFloat(wCtx.arr, wCtx.requestFreeSize(4), val);
    }

    /**
     * @param val Value.
     */
    void doWriteDouble(double val) {
        PRIM.writeDouble(wCtx.arr, wCtx.requestFreeSize(8), val);
    }

    /**
     * @param val Value.
     */
    void doWriteChar(char val) {
        PRIM.writeChar(wCtx.arr, wCtx.requestFreeSize(2), val);
    }

    /**
     * @param val Value.
     */
    void doWriteBoolean(boolean val) {
        PRIM.writeBoolean(wCtx.arr, wCtx.requestFreeSize(1), val);
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
     * @throws GridPortableException In case of error.
     */
    void doWriteObject(@Nullable Object obj) throws GridPortableException {
        if (obj == null)
            doWriteByte(NULL);
        else {
            int handle = wCtx.handle(obj);

            if (handle >= 0) {
                doWriteByte(HANDLE);
                doWriteInt(handle);
            }
            else {
                GridPortableWriterImpl writer = new GridPortableWriterImpl(ctx, wCtx);

                writer.marshal(obj);
            }
        }
    }

    /**
     * @param val Byte array.
     */
    void doWriteByteArray(@Nullable byte[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(val.length), val.length);
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, SHORT_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, INT_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
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

            UNSAFE.copyMemory(val, LONG_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 2;

            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 3;

            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int bytes = val.length << 1;

            UNSAFE.copyMemory(val, CHAR_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(bytes), bytes);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null)
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, wCtx.arr, BYTE_ARR_OFF + wCtx.requestFreeSize(val.length),
                val.length);
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
        writeFieldId(fieldName);

        doWriteInt(2);
        doWriteByte(BYTE);
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws GridPortableException {
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(3);
        doWriteByte(SHORT);
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws GridPortableException {
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(5);
        doWriteByte(INT);
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws GridPortableException {
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(9);
        doWriteByte(LONG);
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws GridPortableException {
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(5);
        doWriteByte(FLOAT);
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws GridPortableException {
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(9);
        doWriteByte(DOUBLE);
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws GridPortableException {
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(3);
        doWriteByte(CHAR);
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws GridPortableException {
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(2);
        doWriteByte(BOOLEAN);
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws GridPortableException {
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws GridPortableException {
        writeFieldId(fieldName);

        byte[] arr = null;
        int len = 0;

        if (val != null) {
            arr = val.getBytes(UTF_8);
            len += arr.length;
        }

        doWriteInt(5 + len);
        doWriteByte(STRING);
        doWriteByteArray(arr);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws GridPortableException {
        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 18 : 2);
        doWriteByte(UUID);
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws GridPortableException {
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteObject(obj);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws GridPortableException {
        doWriteObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + val.length : 5);
        doWriteByte(BYTE_ARR);
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws GridPortableException {
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 1) : 5);
        doWriteByte(SHORT_ARR);
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws GridPortableException {
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 2) : 5);
        doWriteByte(INT_ARR);
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws GridPortableException {
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 3) : 5);
        doWriteByte(LONG_ARR);
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws GridPortableException {
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 2) : 5);
        doWriteByte(FLOAT_ARR);
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws GridPortableException {
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 3) : 5);
        doWriteByte(DOUBLE_ARR);
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws GridPortableException {
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + (val.length << 1) : 5);
        doWriteByte(CHAR_ARR);
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws GridPortableException {
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws GridPortableException {
        writeFieldId(fieldName);

        doWriteInt(val != null ? 5 + val.length : 5);
        doWriteByte(BOOLEAN_ARR);
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException {
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteByte(STRING_ARR);
        doWriteStringArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws GridPortableException {
        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteByte(UUID_ARR);
        doWriteUuidArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException {
        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteByte(OBJ_ARR);
        doWriteObjectArray(val);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws GridPortableException {
        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteByte(COL);
        doWriteCollection(col);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException {
        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws GridPortableException {
        writeFieldId(fieldName);

        int lenPos = reserveAndMark(4);

        doWriteByte(MAP);
        doWriteMap(map);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException {
        doWriteMap(map);
    }

    /** {@inheritDoc} */
    @Override public GridPortableRawWriter rawWriter() {
        PRIM.writeInt(wCtx.arr, start + RAW_DATA_OFF_POS, wCtx.off - start);

        allowFields = false;

        return this;
    }

    /**
     * @throws GridPortableException If fields are not allowed.
     * @param fieldName
     */
    private void writeFieldId(String fieldName) throws GridPortableException {
        A.notNull(fieldName, "fieldName");

        if (!allowFields)
            throw new GridPortableException("Fields are not allowed."); // TODO: proper message

        doWriteInt(ctx.fieldId(typeId, fieldName));
    }

    /** */
    private static class WriterContext {
        /** */
        private byte[] arr;

        /** */
        private int off;

        /** */
        private Map<Object, Integer> handles = new IdentityHashMap<>();

        /**
         */
        private WriterContext() {
            arr = new byte[INIT_CAP];
        }

        /**
         * @param bytes Number of bytes that are going to be written.
         * @return Offset before write.
         */
        private int requestFreeSize(int bytes) {
            int off0 = off;

            off += bytes;

            if (off > arr.length) {
                byte[] arr0 = new byte[off << 1];

                UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, off0);

                arr = arr0;
            }

            return off0;
        }

        /**
         * @param obj Object.
         * @return Handle.
         */
        private int handle(Object obj) {
            assert obj != null;

            Integer h = handles.get(obj);

            if (h != null)
                return h;
            else {
                handles.put(obj, off);

                return -1;
            }
        }
    }
}
