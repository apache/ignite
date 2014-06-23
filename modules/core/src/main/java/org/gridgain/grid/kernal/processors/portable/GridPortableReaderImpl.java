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
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.util.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.kernal.processors.portable.GridPortableMarshaller.*;

/**
 * Portable reader implementation.
 */
class GridPortableReaderImpl implements GridPortableReader, GridPortableRawReader {
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
    private static final int HDR_LEN = 18;

    /** */
    private final GridPortableContext ctx;

    /** */
    private final Map<Integer, GridPortableObject> poHandles;

    /** */
    private final Map<Integer, Object> oHandles;

    /** */
    private final byte[] arr;

    /** */
    private final int start;

    /** */
    private int rawOff;

    /** */
    private int off;

    /** */
    private Map<Integer, Integer> fieldsOffs;

    /**
     * @param arr Array.
     */
    GridPortableReaderImpl(GridPortableContext ctx, byte[] arr) {
        this.ctx = ctx;
        this.arr = arr;

        poHandles = new HashMap<>();
        oHandles = new HashMap<>();

        start = 0;
    }

    /**
     * @param arr Array.
     */
    private GridPortableReaderImpl(GridPortableContext ctx, Map<Integer, GridPortableObject> poHandles,
        Map<Integer, Object> oHandles, byte[] arr, int start, int rawOff) {
        this.ctx = ctx;
        this.poHandles = poHandles;
        this.oHandles = oHandles;
        this.arr = arr;
        this.start = start;
        this.rawOff = rawOff;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unmarshal(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        return off >= 0 ? unmarshal() : null;
    }

    /**
     * @return Unmarshalled value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unmarshal() throws GridPortableException {
        assert off >= 0;

        int start = off;

        byte flag = doReadByte(false);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = doReadInt(false);

                if (poHandles.containsKey(handle))
                    return poHandles.get(handle);

                off = handle;

                return unmarshal();

            case OBJ:
                boolean userType = doReadBoolean(false);
                int typeId = doReadInt(false);
                int hashCode = doReadInt(false);

                // Skip length.
                off += 4;

                rawOff = doReadInt(false);

                GridPortableObject po = new GridPortableObjectImpl(ctx, this, userType, typeId, hashCode);

                poHandles.put(start, po);

                return po;

            case BYTE:
                return doReadByte(false);

            case SHORT:
                return doReadShort(false);

            case INT:
                return doReadInt(false);

            case LONG:
                return doReadLong(false);

            case FLOAT:
                return doReadFloat(false);

            case DOUBLE:
                return doReadDouble(false);

            case CHAR:
                return doReadChar(false);

            case BOOLEAN:
                return doReadBoolean(false);

            case STRING:
                return doReadString(false);

            case UUID:
                return doReadUuid(false);

            case BYTE_ARR:
                return doReadByteArray(false);

            case SHORT_ARR:
                return doReadShortArray(false);

            case INT_ARR:
                return doReadIntArray(false);

            case LONG_ARR:
                return doReadLongArray(false);

            case FLOAT_ARR:
                return doReadFloatArray(false);

            case DOUBLE_ARR:
                return doReadDoubleArray(false);

            case CHAR_ARR:
                return doReadCharArray(false);

            case BOOLEAN_ARR:
                return doReadBooleanArray(false);

            case STRING_ARR:
                return doReadStringArray(false);

            case UUID_ARR:
                return doReadUuidArray(false);

            case OBJ_ARR:
                int len = doReadInt(false);

                if (len >= 0) {
                    Object[] arr = new Object[len];

                    for (int i = 0; i < len; i++)
                        arr[i] = unmarshal();

                    return arr;
                }
                else
                    return null;

            case COL:
                int colSize = doReadInt(false);

                if (colSize >= 0) {
                    Collection<Object> col = new ArrayList<>(colSize);

                    for (int i = 0; i < colSize; i++)
                        col.add(unmarshal());

                    return col;
                }
                else
                    return null;

            case MAP:
                int mapSize = doReadInt(false);

                if (mapSize >= 0) {
                    Map<Object, Object> map = new HashMap<>(mapSize);

                    for (int i = 0; i < mapSize; i++)
                        map.put(unmarshal(), unmarshal());

                    return map;
                }
                else
                    return null;

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BYTE)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadByte(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws GridPortableException {
        return doReadByte(true);
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != SHORT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadShort(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws GridPortableException {
        return doReadShort(true);
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != INT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadInt(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws GridPortableException {
        return doReadInt(true);
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != LONG)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadLong(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws GridPortableException {
        return doReadLong(true);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != FLOAT)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadFloat(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws GridPortableException {
        return doReadFloat(true);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DOUBLE)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDouble(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws GridPortableException {
        return doReadDouble(true);
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != CHAR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadChar(false);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws GridPortableException {
        return doReadChar(true);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BOOLEAN)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadBoolean(false);
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws GridPortableException {
        return doReadBoolean(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != STRING)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadString(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws GridPortableException {
        return doReadString(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != UUID)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadUuid(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws GridPortableException {
        return doReadUuid(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        return off >= 0 ? doReadObject(false) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject() throws GridPortableException {
        return doReadObject(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BYTE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadByteArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        return doReadByteArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != SHORT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadShortArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        return doReadShortArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != INT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadIntArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        return doReadIntArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != LONG_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadLongArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        return doReadLongArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != FLOAT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadFloatArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        return doReadFloatArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != DOUBLE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadDoubleArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        return doReadDoubleArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != CHAR_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadCharArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        return doReadCharArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BOOLEAN_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadBooleanArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        return doReadBooleanArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != STRING_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadStringArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws GridPortableException {
        return doReadStringArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != UUID_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadUuidArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws GridPortableException {
        return doReadUuidArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != OBJ_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return doReadObjectArray(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws GridPortableException {
        return doReadObjectArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != COL)
                throw new GridPortableException("Invalid flag value: " + flag);

            return (Collection<T>)doReadCollection(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws GridPortableException {
        return (Collection<T>)doReadCollection(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws GridPortableException {
        off = fieldOffset(fieldName);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag != BOOLEAN_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return (Map<K, V>)doReadMap(false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws GridPortableException {
        return (Map<K, V>)doReadMap(true);
    }

    /** {@inheritDoc} */
    @Override public GridPortableRawReader rawReader() {
        return this;
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
     * @return Object.
     * @throws GridPortableException In case of error.
     */
    @Nullable private Object doReadObject(boolean raw) throws GridPortableException {
        int start = raw ? rawOff : off;

        byte flag = doReadByte(true);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = doReadInt(false);

                if (oHandles.containsKey(handle))
                    return oHandles.get(handle);

                off = handle;

                return doReadObject(false);

            case OBJ:
                boolean userType = doReadBoolean(raw);
                int typeId = doReadInt(raw);

                GridPortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId);

                if (desc == null)
                    throw new GridPortableInvalidClassException("Unknown type ID: " + typeId);

                // Skip hash code.
                if (raw)
                    rawOff += 4;
                else
                    off += 4;

                int dataLen = doReadInt(raw) - HDR_LEN;
                int rawOff0 = start + doReadInt(raw);

                Object obj = desc.read(new GridPortableReaderImpl(ctx, poHandles, oHandles, arr, start, rawOff0));

                if (raw)
                    rawOff += dataLen;
                else
                    off += dataLen;

                oHandles.put(start, obj);

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
            byte[] arr = new byte[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, BYTE_ARR_OFF, len);

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
            short[] arr = new short[len];

            int bytes = len << 1;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, SHORT_ARR_OFF, bytes);

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
            int[] arr = new int[len];

            int bytes = len << 2;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, INT_ARR_OFF, bytes);

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
            long[] arr = new long[len];

            int bytes = len << 3;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, LONG_ARR_OFF, bytes);

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
            float[] arr = new float[len];

            int bytes = len << 2;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, FLOAT_ARR_OFF, bytes);

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
            double[] arr = new double[len];

            int bytes = len << 3;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, DOUBLE_ARR_OFF, bytes);

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
            char[] arr = new char[len];

            int bytes = len << 1;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, CHAR_ARR_OFF, bytes);

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
            boolean[] arr = new boolean[len];

            int bytes = len << 1;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + (raw ? rawOff : off), arr, BOOLEAN_ARR_OFF, bytes);

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
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    private Collection<?> doReadCollection(boolean raw) throws GridPortableException {
        int size = doReadInt(raw);

        if (size >= 0) {
            Collection<Object> col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add(doReadObject(raw));

            return col;
        }
        else
            return null;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws GridPortableException In case of error.
     */
    private Map<?, ?> doReadMap(boolean raw) throws GridPortableException {
        int size = doReadInt(raw);

        if (size >= 0) {
            Map<Object, Object> col = new HashMap<>(size);

            for (int i = 0; i < size; i++)
                col.put(doReadObject(raw), doReadObject(raw));

            return col;
        }
        else
            return null;
    }

    /**
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldOffset(String name) {
        assert name != null;

        if (fieldsOffs == null) {
            fieldsOffs = new HashMap<>();

            int off = start + HDR_LEN;

            while (true) {
                if (off >= arr.length)
                    break;

                int id0 = PRIM.readInt(arr, off);

                off += 4;

                int len = PRIM.readInt(arr, off);

                off += 4;

                fieldsOffs.put(id0, off);

                off += len;
            }
        }

        Integer fieldOff = fieldsOffs.get(name.hashCode()); // TODO: get id from mapper

        return fieldOff != null ? fieldOff : -1;
    }
}
