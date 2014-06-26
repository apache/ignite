/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.util.portable.GridPortableMarshaller.*;

/**
 * Portable writer implementation.
 */
class GridPortableWriterImpl implements GridPortableWriter, GridPortableRawWriter {
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
    private Class<?> cls;

    /** */
    private int typeId;

    /** */
    private boolean allowFields = true;

    /**
     * @param ctx Context.
     * @param off Start offset.
     */
    GridPortableWriterImpl(GridPortableContext ctx, int off) {
        this.ctx = ctx;

        wCtx = new WriterContext(off);

        start = off;
    }

    /**
     * @param ctx Context.
     * @param wCtx Writer context.
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

        int handle = wCtx.handle(obj);

        if (handle >= 0) {
            doWriteByte(HANDLE);
            doWriteInt(handle);
        }
        else {
            doWriteByte(OBJ);

            cls = obj.getClass();

            GridPortableClassDescriptor desc = ctx.descriptorForClass(cls);

            if (desc == null) throw new GridPortableException("Object is not portable: " + obj);

            typeId = desc.typeId();

            desc.write(obj, this);
        }
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

        int off = wCtx.requestFreeSize(val.length);

        PRIM.writeByteArray(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteByte(byte val) {
        int off = wCtx.requestFreeSize(1);

        PRIM.writeByte(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteShort(short val) {
        int off = wCtx.requestFreeSize(2);

        PRIM.writeShort(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteInt(int val) {
        int off = wCtx.requestFreeSize(4);

        PRIM.writeInt(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteLong(long val) {
        int off = wCtx.requestFreeSize(8);

        PRIM.writeLong(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteFloat(float val) {
        int off = wCtx.requestFreeSize(4);

        PRIM.writeFloat(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteDouble(double val) {
        int off = wCtx.requestFreeSize(8);

        PRIM.writeDouble(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteChar(char val) {
        int off = wCtx.requestFreeSize(2);

        PRIM.writeChar(wCtx.arr, off, val);
    }

    /**
     * @param val Value.
     */
    void doWriteBoolean(boolean val) {
        int off = wCtx.requestFreeSize(1);

        PRIM.writeBoolean(wCtx.arr, off, val);
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
    * @param date Date.
    */
   void doWriteDate(@Nullable Date date) {
       if (date == null)
           doWriteBoolean(false);
       else {
           doWriteBoolean(true);
           doWriteLong(date.getTime());
           doWriteShort((short)0);
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
            GridPortableWriterImpl writer = new GridPortableWriterImpl(ctx, wCtx);

            writer.marshal(obj);
        }
    }

    /**
     * @param val Byte array.
     */
    void doWriteByteArray(@Nullable byte[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length);

            PRIM.writeByteArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 1);

            PRIM.writeShortArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 2);

            PRIM.writeIntArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Long array.
     * @throws GridPortableException In case of error.
     */
    void doWriteLongArray(@Nullable long[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 3);

            PRIM.writeLongArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 2);

            PRIM.writeFloatArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 3);

            PRIM.writeDoubleArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length << 1);

            PRIM.writeCharArray(wCtx.arr, off, val);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            int off = wCtx.requestFreeSize(val.length);

            PRIM.writeBooleanArray(wCtx.arr, off, val);
        }
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
     * @param val Array of dates.
     */
    void doWriteDateArray(@Nullable Date[] val) {
        doWriteInt(val != null ? val.length : -1);

        if (val != null) {
            for (Date date : val)
                doWriteDate(date);
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
            doWriteByte(ctx.collectionType(col.getClass()));

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
            doWriteByte(ctx.mapType(map.getClass()));

            for (Map.Entry<K, V> e : map.entrySet()) {
                doWriteObject(e.getKey());
                doWriteObject(e.getValue());
            }
        }
    }

    /**
     * @param val Value.
     */
    void writeByteField(byte val) {
        doWriteInt(2);
        doWriteByte(BYTE);
        doWriteByte(val);
    }

    /**
     * @param val Value.
     */
    void writeShortField(short val) {
        doWriteInt(3);
        doWriteByte(SHORT);
        doWriteShort(val);
    }

    /**
     * @param val Value.
     */
    void writeIntField(int val) {
        doWriteInt(5);
        doWriteByte(INT);
        doWriteInt(val);
    }

    /**
     * @param val Value.
     */
    void writeLongField(long val) {
        doWriteInt(9);
        doWriteByte(LONG);
        doWriteLong(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatField(float val) {
        doWriteInt(5);
        doWriteByte(FLOAT);
        doWriteFloat(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleField(double val) {
        doWriteInt(9);
        doWriteByte(DOUBLE);
        doWriteDouble(val);
    }

    /**
     * @param val Value.
     */
    void writeCharField(char val) {
        doWriteInt(3);
        doWriteByte(CHAR);
        doWriteChar(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanField(boolean val) {
        doWriteInt(2);
        doWriteByte(BOOLEAN);
        doWriteBoolean(val);
    }

    /**
     * @param val Value.
     */
    void writeStringField(@Nullable String val) {
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

    /**
     * @param val Value.
     */
    void writeUuidField(@Nullable UUID val) {
        doWriteInt(val != null ? 18 : 2);
        doWriteByte(UUID);
        doWriteUuid(val);
    }

    /**
     * @param val Value.
     */
    void writeDateField(@Nullable Date val) {
        doWriteInt(val != null ? 10 : 2);
        doWriteByte(DATE);
        doWriteDate(val);
    }

    /**
     * @param obj Object.
     * @throws GridPortableException In case of error.
     */
    void writeObjectField(@Nullable Object obj) throws GridPortableException {
        int lenPos = reserveAndMark(4);

        doWriteObject(obj);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeByteArrayField(@Nullable byte[] val) {
        doWriteInt(val != null ? 5 + val.length : 5);
        doWriteByte(BYTE_ARR);
        doWriteByteArray(val);
    }

    /**
     * @param val Value.
     */
    void writeShortArrayField(@Nullable short[] val) {
        doWriteInt(val != null ? 5 + (val.length << 1) : 5);
        doWriteByte(SHORT_ARR);
        doWriteShortArray(val);
    }

    /**
     * @param val Value.
     */
    void writeIntArrayField(@Nullable int[] val) {
        doWriteInt(val != null ? 5 + (val.length << 2) : 5);
        doWriteByte(INT_ARR);
        doWriteIntArray(val);
    }

    /**
     * @param val Value.
     */
    void writeLongArrayField(@Nullable long[] val) {
        doWriteInt(val != null ? 5 + (val.length << 3) : 5);
        doWriteByte(LONG_ARR);
        doWriteLongArray(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatArrayField(@Nullable float[] val) {
        doWriteInt(val != null ? 5 + (val.length << 2) : 5);
        doWriteByte(FLOAT_ARR);
        doWriteFloatArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleArrayField(@Nullable double[] val) {
        doWriteInt(val != null ? 5 + (val.length << 3) : 5);
        doWriteByte(DOUBLE_ARR);
        doWriteDoubleArray(val);
    }

    /**
     * @param val Value.
     */
    void writeCharArrayField(@Nullable char[] val) {
        doWriteInt(val != null ? 5 + (val.length << 1) : 5);
        doWriteByte(CHAR_ARR);
        doWriteCharArray(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanArrayField(@Nullable boolean[] val) {
        doWriteInt(val != null ? 5 + val.length : 5);
        doWriteByte(BOOLEAN_ARR);
        doWriteBooleanArray(val);
    }

    /**
     * @param val Value.
     */
    void writeStringArrayField(@Nullable String[] val) {
        int lenPos = reserveAndMark(4);

        doWriteByte(STRING_ARR);
        doWriteStringArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeUuidArrayField(@Nullable UUID[] val) {
        int lenPos = reserveAndMark(4);

        doWriteByte(UUID_ARR);
        doWriteUuidArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     */
    void writeDateArrayField(@Nullable Date[] val) {
        int lenPos = reserveAndMark(4);

        doWriteByte(DATE_ARR);
        doWriteDateArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param val Value.
     * @throws GridPortableException In case of error.
     */
    void writeObjectArrayField(@Nullable Object[] val) throws GridPortableException {
        int lenPos = reserveAndMark(4);

        doWriteByte(OBJ_ARR);
        doWriteObjectArray(val);

        writeDelta(lenPos);
    }

    /**
     * @param col Collection.
     * @throws GridPortableException In case of error.
     */
    void writeCollectionField(@Nullable Collection<?> col) throws GridPortableException {
        int lenPos = reserveAndMark(4);

        doWriteByte(COL);
        doWriteCollection(col);

        writeDelta(lenPos);
    }

    /**
     * @param map Map.
     * @throws GridPortableException In case of error.
     */
    void writeMapField(@Nullable Map<?, ?> map) throws GridPortableException {
        int lenPos = reserveAndMark(4);

        doWriteByte(MAP);
        doWriteMap(map);

        writeDelta(lenPos);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws GridPortableException {
        writeFieldId(fieldName);
        writeByteField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws GridPortableException {
        doWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws GridPortableException {
        writeFieldId(fieldName);
        writeShortField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws GridPortableException {
        doWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws GridPortableException {
        writeFieldId(fieldName);
        writeIntField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws GridPortableException {
        doWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws GridPortableException {
        writeFieldId(fieldName);
        writeLongField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws GridPortableException {
        doWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws GridPortableException {
        writeFieldId(fieldName);
        writeFloatField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws GridPortableException {
        doWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws GridPortableException {
        writeFieldId(fieldName);
        writeDoubleField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws GridPortableException {
        doWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws GridPortableException {
        writeFieldId(fieldName);
        writeCharField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws GridPortableException {
        doWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws GridPortableException {
        writeFieldId(fieldName);
        writeBooleanField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws GridPortableException {
        doWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws GridPortableException {
        writeFieldId(fieldName);
        writeStringField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws GridPortableException {
        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws GridPortableException {
        writeFieldId(fieldName);
        writeUuidField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws GridPortableException {
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws GridPortableException {
        writeFieldId(fieldName);
        writeDateField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(@Nullable Date val) throws GridPortableException {
        doWriteDate(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws GridPortableException {
        writeFieldId(fieldName);
        writeObjectField(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws GridPortableException {
        doWriteObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeByteArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws GridPortableException {
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeShortArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws GridPortableException {
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeIntArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws GridPortableException {
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeLongArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws GridPortableException {
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeFloatArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws GridPortableException {
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws GridPortableException {
        writeFieldId(fieldName);
        writeDoubleArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws GridPortableException {
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeCharArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws GridPortableException {
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws GridPortableException {
        writeFieldId(fieldName);
        writeBooleanArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException {
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws GridPortableException {
        writeFieldId(fieldName);
        writeStringArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws GridPortableException {
        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeUuidArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException {
        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeDateArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(@Nullable Date[] val) throws GridPortableException {
        doWriteDateArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException {
        writeFieldId(fieldName);
        writeObjectArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws GridPortableException {
        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws GridPortableException {
        writeFieldId(fieldName);
        writeCollectionField(col);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException {
        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws GridPortableException {
        writeFieldId(fieldName);
        writeMapField(map);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException {
        doWriteMap(map);
    }

    /** {@inheritDoc} */
    @Override public GridPortableRawWriter rawWriter() {
        if (allowFields) {
            PRIM.writeInt(wCtx.arr, start + RAW_DATA_OFF_POS, wCtx.off - start);

            allowFields = false;
        }

        return this;
    }

    /**
     * @throws GridPortableException If fields are not allowed.
     * @param fieldName
     */
    private void writeFieldId(String fieldName) throws GridPortableException {
        A.notNull(fieldName, "fieldName");

        if (!allowFields)
            throw new GridPortableException("Individual field can't be written after raw writer is acquired " +
                "via rawWriter() method. Consider fixing serialization logic for class: " + cls.getName());

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
         * @param off Start offset.
         */
        private WriterContext(int off) {
            arr = new byte[off + INIT_CAP];

            this.off = off;
        }

        /**
         * @param bytes Number of bytes that are going to be written.
         * @return Offset before write.
         */
        private int requestFreeSize(int bytes) {
            int off0 = off;

            off += bytes;

            if (off >= arr.length) {
                byte[] arr0 = new byte[off << 1];

                U.arrayCopy(arr, 0, arr0, 0, arr.length);

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
