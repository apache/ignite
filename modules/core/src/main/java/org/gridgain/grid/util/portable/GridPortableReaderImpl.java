/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.util.portable.GridPortableMarshaller.*;

/**
 * Portable reader implementation.
 */
class GridPortableReaderImpl implements GridPortableReader, GridPortableRawReader {
    /** */
    private static final GridPortablePrimitives PRIM = GridPortablePrimitives.get();

    /** */
    private static final int HDR_LEN = 18;

    /** */
    private final GridPortableContext ctx;

    /** */
    private final Map<Integer, GridPortableObject<?>> poHandles;

    /** */
    private final Map<Integer, Object> oHandles;

    /** */
    private final byte[] arr;

    /** */
    private final int start;

    /** */
    private byte flag;

    /** */
    private int handle;

    /** */
    private boolean userType;

    /** */
    private int typeId;

    /** */
    private int hashCode;

    /** */
    private int len;

    /** */
    private int rawStart;

    /** */
    private Map<Integer, Integer> fieldOffs;

    /** */
    private int off;

    /** */
    private int rawOff;

    /**
     * @param ctx Context.
     * @param arr Array.
     * @param start Start.
     */
    GridPortableReaderImpl(GridPortableContext ctx, byte[] arr, int start) {
        this(ctx, arr, start, new HashMap<Integer, GridPortableObject<?>>(), new HashMap<Integer, Object>());
    }

    /**
     * @param ctx Context.
     * @param arr Array.
     * @param start Start.
     * @param poHandles Portable handles.
     * @param oHandles Object handles.
     */
    private GridPortableReaderImpl(GridPortableContext ctx, byte[] arr, int start,
        Map<Integer, GridPortableObject<?>> poHandles, Map<Integer, Object> oHandles) {
        this.ctx = ctx;
        this.arr = arr;
        this.start = start;
        this.poHandles = poHandles;
        this.oHandles = oHandles;

        off = start;

        flag = doReadByte(false);

        switch (flag) {
            case NULL:
                len = 1;

                break;

            case HANDLE:
                handle = doReadInt(false);
                len = 5;

                break;

            case OBJ:
                userType = doReadBoolean(false);
                typeId = doReadInt(false);
                hashCode = doReadInt(false);
                len = doReadInt(false);
                rawStart = start + doReadInt(false);
        }

        rawOff = rawStart;
    }

    /**
     * @return User type flag.
     */
    boolean userType() {
        return userType;
    }

    /**
     * @return Type ID.
     */
    int typeId() {
        return typeId;
    }

    /**
     * @return Hash code.
     */
    int objectHashCode() {
        return hashCode;
    }

    /**
     * @return Length.
     */
    int length() {
        return len;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unmarshal(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldId(fieldName));

        return off >= 0 ? unmarshal(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    byte readByte(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BYTE)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadByte(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    short readShort(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != SHORT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadShort(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    int readInt(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != INT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadInt(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    long readLong(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != LONG)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadLong(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    float readFloat(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != FLOAT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadFloat(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    double readDouble(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DOUBLE)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDouble(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    char readChar(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != CHAR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadChar(false);
        }
        else
            return 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    boolean readBoolean(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BOOLEAN)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadBoolean(false);
        }
        else
            return false;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable String readString(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != STRING)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadString(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable UUID readUuid(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != UUID)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadUuid(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Date readDate(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DATE)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDate(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        return off >= 0 ? doReadObject(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable byte[] readByteArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BYTE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadByteArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable short[] readShortArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != SHORT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadShortArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable int[] readIntArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != INT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadIntArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable long[] readLongArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != LONG_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadLongArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable float[] readFloatArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != FLOAT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadFloatArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable double[] readDoubleArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DOUBLE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDoubleArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable char[] readCharArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != CHAR_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadCharArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable boolean[] readBooleanArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BOOLEAN_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadBooleanArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable String[] readStringArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != STRING_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadStringArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable UUID[] readUuidArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != UUID_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadUuidArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Date[] readDateArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DATE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDateArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object[] readObjectArray(int fieldId) throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != OBJ_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadObjectArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Collection class.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable <T> Collection<T> readCollection(int fieldId, @Nullable Class<? extends Collection> cls)
        throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != COL)
                throw new GridPortableException("Invalid flag value: " + flag);

            return (Collection<T>)doReadCollection(false, cls);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Map class.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @Nullable <K, V> Map<K, V> readMap(int fieldId, @Nullable Class<? extends Map> cls)
        throws GridPortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != MAP)
                throw new GridPortableException("Invalid flag value: " + flag);

            return (Map<K, V>)doReadMap(false, cls);
        }
        else
            return null;
    }

    /**
     * @param obj New object.
     */
    void setHandler(Object obj) {
        assert obj != null;

        oHandles.put(start, obj);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws GridPortableException {
        return readByte(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws GridPortableException {
        return doReadByte(true);
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws GridPortableException {
        return readShort(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws GridPortableException {
        return doReadShort(true);
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws GridPortableException {
        return readInt(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws GridPortableException {
        return doReadInt(true);
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws GridPortableException {
        return readLong(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws GridPortableException {
        return doReadLong(true);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws GridPortableException {
        return readFloat(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws GridPortableException {
        return doReadFloat(true);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws GridPortableException {
        return readDouble(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws GridPortableException {
        return doReadDouble(true);
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws GridPortableException {
        return readChar(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws GridPortableException {
        return doReadChar(true);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws GridPortableException {
        return readBoolean(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws GridPortableException {
        return doReadBoolean(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws GridPortableException {
        return readString(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws GridPortableException {
        return doReadString(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws GridPortableException {
        return readUuid(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws GridPortableException {
        return doReadUuid(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate(String fieldName) throws GridPortableException {
        return readDate(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate() throws GridPortableException {
        return doReadDate(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject(String fieldName) throws GridPortableException {
        return readObject(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject() throws GridPortableException {
        return doReadObject(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws GridPortableException {
        return readByteArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        return doReadByteArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        return readShortArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        return doReadShortArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        return readIntArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        return doReadIntArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        return readLongArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        return doReadLongArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        return readFloatArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        return doReadFloatArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        return readDoubleArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        return doReadDoubleArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        return readCharArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        return doReadCharArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        return readBooleanArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        return doReadBooleanArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        return readStringArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws GridPortableException {
        return doReadStringArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws GridPortableException {
        return readUuidArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws GridPortableException {
        return doReadUuidArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray(String fieldName) throws GridPortableException {
        return readDateArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray() throws GridPortableException {
        return doReadDateArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws GridPortableException {
        return readObjectArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws GridPortableException {
        return doReadObjectArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws GridPortableException {
        return readCollection(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws GridPortableException {
        return (Collection<T>)doReadCollection(true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName,
        Class<? extends Collection<T>> colCls) throws GridPortableException {
        return readCollection(fieldId(fieldName), colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(
        Class<? extends Collection<T>> colCls) throws GridPortableException {
        return (Collection<T>)doReadCollection(true, colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws GridPortableException {
        return readMap(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws GridPortableException {
        return (Map<K, V>)doReadMap(true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName,
        Class<? extends Map<K, V>> mapCls) throws GridPortableException {
        return readMap(fieldId(fieldName), mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(
        Class<? extends Map<K, V>> mapCls) throws GridPortableException {
        return (Map<K, V>)doReadMap(true, mapCls);
    }

    /** {@inheritDoc} */
    @Override public GridPortableRawReader rawReader() {
        return this;
    }

    /**
     * @param raw Raw flag.
     * @return Unmarshalled value.
     * @throws GridPortableException In case of error.
     */
    @Nullable private Object unmarshal(boolean raw) throws GridPortableException {
        int start = raw ? rawOff : off;

        byte flag = doReadByte(raw);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = doReadInt(raw);

                if (poHandles.containsKey(handle))
                    return poHandles.get(handle);

                off = handle;

                return unmarshal(false);

            case OBJ:
                GridPortableObjectImpl<?> po = new GridPortableObjectImpl<>(ctx, arr, start);

                poHandles.put(start, po);

                if (raw)
                    rawOff = start + po.length();

                return po;

            case BYTE:
                return doReadByte(raw);

            case SHORT:
                return doReadShort(raw);

            case INT:
                return doReadInt(raw);

            case LONG:
                return doReadLong(raw);

            case FLOAT:
                return doReadFloat(raw);

            case DOUBLE:
                return doReadDouble(raw);

            case CHAR:
                return doReadChar(raw);

            case BOOLEAN:
                return doReadBoolean(raw);

            case STRING:
                return doReadString(raw);

            case UUID:
                return doReadUuid(raw);

            case DATE:
                return doReadDate(raw);

            case BYTE_ARR:
                return doReadByteArray(raw);

            case SHORT_ARR:
                return doReadShortArray(raw);

            case INT_ARR:
                return doReadIntArray(raw);

            case LONG_ARR:
                return doReadLongArray(raw);

            case FLOAT_ARR:
                return doReadFloatArray(raw);

            case DOUBLE_ARR:
                return doReadDoubleArray(raw);

            case CHAR_ARR:
                return doReadCharArray(raw);

            case BOOLEAN_ARR:
                return doReadBooleanArray(raw);

            case STRING_ARR:
                return doReadStringArray(raw);

            case UUID_ARR:
                return doReadUuidArray(raw);

            case DATE_ARR:
                return doReadDateArray(raw);

            case OBJ_ARR:
                return doReadObjectArray(raw);

            case COL:
                return doReadCollection(raw, null);

            case MAP:
                return doReadMap(raw, null);

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private byte doReadByte(boolean raw) {
        return PRIM.readByte(arr, raw ? rawOff++ : off++);
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private short doReadShort(boolean raw) {
        short val = PRIM.readShort(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 2;
        else
            off += 2;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private int doReadInt(boolean raw) {
        int val = PRIM.readInt(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 4;
        else
            off += 4;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private long doReadLong(boolean raw) {
        long val = PRIM.readLong(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 8;
        else
            off += 8;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private float doReadFloat(boolean raw) {
        float val = PRIM.readFloat(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 4;
        else
            off += 4;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private double doReadDouble(boolean raw) {
        double val = PRIM.readDouble(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 8;
        else
            off += 8;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private char doReadChar(boolean raw) {
        char val = PRIM.readChar(arr, raw ? rawOff : off);

        if (raw)
            rawOff += 2;
        else
            off += 2;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private boolean doReadBoolean(boolean raw) {
        return PRIM.readBoolean(arr, raw ? rawOff++ : off++);
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private String doReadString(boolean raw) {
        byte[] arr = doReadByteArray(raw);

        return arr != null ? new String(arr, UTF_8) : null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private UUID doReadUuid(boolean raw) {
        if (doReadBoolean(raw)) {
            long most = doReadLong(raw);
            long least = doReadLong(raw);

            return new UUID(most, least);
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Date doReadDate(boolean raw) {
        if (doReadBoolean(raw)) {
            long time = doReadLong(raw);

            // Skip remainder.
            if (raw)
                rawOff += 2;
            else
                off += 2;

            return new Date(time);
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Object.
     * @throws GridPortableException In case of error.
     */
    @Nullable private Object doReadObject(boolean raw) throws GridPortableException {
        GridPortableReaderImpl reader = new GridPortableReaderImpl(ctx, arr, raw ? rawOff : off, poHandles, oHandles);

        if (raw)
            rawOff += reader.length();
        else
            off += reader.length();

        return reader.deserialize();
    }

    /**
     * @return Deserialized object.
     * @throws GridPortableException
     */
    Object deserialize() throws GridPortableException {
        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                if (oHandles.containsKey(handle))
                    return oHandles.get(handle);

                off = handle;

                return doReadObject(false);

            case OBJ:
                GridPortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId);

                if (desc == null)
                    throw new GridPortableInvalidClassException("Unknown type ID: " + typeId);

                Object obj = desc.read(this);

                if (obj instanceof GridPortableObjectImpl)
                    ((GridPortableObjectImpl)obj).context(ctx);

                return obj;

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private byte[] doReadByteArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            byte[] arr = PRIM.readByteArray(this.arr, raw ? rawOff : off, len);

            if (raw)
                rawOff += len;
            else
                off += len;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private short[] doReadShortArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            short[] arr = PRIM.readShortArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 1;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private int[] doReadIntArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            int[] arr = PRIM.readIntArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 2;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private long[] doReadLongArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            long[] arr = PRIM.readLongArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 3;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private float[] doReadFloatArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            float[] arr = PRIM.readFloatArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 2;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private double[] doReadDoubleArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            double[] arr = PRIM.readDoubleArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 3;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private char[] doReadCharArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            char[] arr = PRIM.readCharArray(this.arr, raw ? rawOff : off, len);

            int bytes = len << 1;

            if (raw)
                rawOff += bytes;
            else
                off += bytes;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private boolean[] doReadBooleanArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            boolean[] arr = PRIM.readBooleanArray(this.arr, raw ? rawOff : off, len);

            if (raw)
                rawOff += len;
            else
                off += len;

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private String[] doReadStringArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            String[] arr = new String[len];

            for (int i = 0; i < len; i++)
                arr[i] = doReadString(raw);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private UUID[] doReadUuidArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            UUID[] arr = new UUID[len];

            for (int i = 0; i < len; i++)
                arr[i] = doReadUuid(raw);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Date[] doReadDateArray(boolean raw) {
        int len = doReadInt(raw);

        if (len >= 0) {
            Date[] arr = new Date[len];

            for (int i = 0; i < len; i++)
                arr[i] = doReadDate(raw);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    private Object[] doReadObjectArray(boolean raw) throws GridPortableException {
        int len = doReadInt(raw);

        if (len >= 0) {
            Object[] arr = new Object[len];

            for (int i = 0; i < len; i++)
                arr[i] = doReadObject(raw);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @param cls Collection class.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Collection<?> doReadCollection(boolean raw, @Nullable Class<? extends Collection> cls)
        throws GridPortableException {
        int size = doReadInt(raw);

        if (size >= 0) {
            byte colType = doReadByte(raw);

            Collection<Object> col;

            if (cls != null) {
                try {
                    Constructor<? extends Collection> cons = cls.getConstructor();

                    col = cons.newInstance();
                }
                catch (NoSuchMethodException ignored) {
                    throw new GridPortableException("Collection class doesn't have public default constructor: " +
                        cls.getName());
                }
                catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                    throw new GridPortableException("Failed to instantiate collection: " + cls.getName(), e);
                }
            }
            else {
                switch (colType) {
                    case ARR_LIST:
                        col = new ArrayList<>(size);

                        break;

                    case LINKED_LIST:
                        col = new LinkedList<>();

                        break;

                    case HASH_SET:
                        col = new HashSet<>(size);

                        break;

                    case LINKED_HASH_SET:
                        col = new LinkedHashSet<>(size);

                        break;

                    case TREE_SET:
                        col = new TreeSet<>();

                        break;

                    case CONC_SKIP_LIST_SET:
                        col = new ConcurrentSkipListSet<>();

                        break;

                    default:
                        assert colType == USER_COL;

                        col = new ArrayList<>(size);
                }
            }

            for (int i = 0; i < size; i++)
                col.add(doReadObject(raw));

            return col;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @param cls Map class.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Map<?, ?> doReadMap(boolean raw, @Nullable Class<? extends Map> cls) throws GridPortableException {
        int size = doReadInt(raw);

        if (size >= 0) {
            byte mapType = doReadByte(raw);

            Map<Object, Object> map;

            if (cls != null) {
                try {
                    Constructor<? extends Map> cons = cls.getConstructor();

                    map = cons.newInstance();
                }
                catch (NoSuchMethodException ignored) {
                    throw new GridPortableException("Map class doesn't have public default constructor: " +
                        cls.getName());
                }
                catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                    throw new GridPortableException("Failed to instantiate map: " + cls.getName(), e);
                }
            }
            else {
                switch (mapType) {
                    case HASH_MAP:
                        map = new HashMap<>(size);

                        break;

                    case LINKED_HASH_MAP:
                        map = new LinkedHashMap<>(size);

                        break;

                    case TREE_MAP:
                        map = new TreeMap<>();

                        break;

                    case CONC_HASH_MAP:
                        map = new ConcurrentHashMap<>(size);

                        break;

                    default:
                        assert mapType == USER_COL;

                        map = new HashMap<>(size);
                }
            }

            for (int i = 0; i < size; i++)
                map.put(doReadObject(raw), doReadObject(raw));

            return map;
        }
        else
            return null;
    }

    /**
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldId(String name) {
        assert name != null;

        return ctx.fieldId(typeId, name);
    }

    /**
     * @param id Field ID.
     * @return Field offset.
     */
    private int fieldOffset(int id) {
        if (fieldOffs == null) {
            fieldOffs = new HashMap<>();

            int off = start + HDR_LEN;

            while (true) {
                if (off >= rawStart)
                    break;

                int id0 = PRIM.readInt(arr, off);

                off += 4;

                int len = PRIM.readInt(arr, off);

                off += 4;

                fieldOffs.put(id0, off);

                off += len;
            }
        }

        Integer fieldOff = fieldOffs.get(id);

        return fieldOff != null ? fieldOff : -1;
    }
}
