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
    private final GridPortableContext ctx;

    /** */
    private final byte[] arr;

    /** */
    private final int start;

    /** */
    private int rawDataOff;

    /** */
    private Map<Integer, Integer> fieldsOffs;

    /**
     * @param arr Array.
     */
    GridPortableReaderImpl(GridPortableContext ctx, byte[] arr) {
        this.ctx = ctx;
        this.arr = arr;

        start = 0;
    }

    /**
     * @param arr Array.
     */
    private GridPortableReaderImpl(GridPortableContext ctx, byte[] arr, int start, int rawDataOff) {
        this.ctx = ctx;
        this.arr = arr;
        this.start = start;
        this.rawDataOff = rawDataOff;
    }

    /**
     * @param fieldName Field name.
     * @return Field value.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unmarshalField(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? unmarshal(fieldOff) : null;
    }

    /**
     * @param off Offset.
     * @return Object.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unmarshal(int off) throws GridPortableException {
        byte flag = readByte(off++);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                // TODO: Handle.

                return null;

            case OBJ:
                boolean userType = readBoolean(off);
                int typeId = readInt(off + 1);
                int hashCode = readInt(off + 5);

                rawDataOff = readInt(off + 13);

                return new GridPortableObjectImpl(ctx, this, userType, typeId, hashCode);

            case BYTE:
                return readByte(off);

            case SHORT:
                return readShort(off);

            case INT:
                return readInt(off);

            case LONG:
                return readLong(off);

            case FLOAT:
                return readFloat(off);

            case DOUBLE:
                return readDouble(off);

            case CHAR:
                return readChar(off);

            case BOOLEAN:
                return readBoolean(off);

            case STRING:
                return readString(off);

            case UUID:
                return readUuid(off);

            case BYTE_ARR:
                return readByteArray(off);

            case SHORT_ARR:
                return readShortArray(off);

            case INT_ARR:
                return readIntArray(off);

            case LONG_ARR:
                return readLongArray(off);

            case FLOAT_ARR:
                return readFloatArray(off);

            case DOUBLE_ARR:
                return readDoubleArray(off);

            case CHAR_ARR:
                return readCharArray(off);

            case BOOLEAN_ARR:
                return readBooleanArray(off);


            // TODO: Others
//            case STRING_ARR:
//                return readStringArray(off);
//
//            case UUID:
//                return readByte(off);

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != BYTE)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readByte(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws GridPortableException {
        byte val = readByte(rawDataOff);

        rawDataOff++;

        return val;
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != SHORT)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readShort(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws GridPortableException {
        short val = readShort(rawDataOff);

        rawDataOff += 2;

        return val;
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != INT)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readInt(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws GridPortableException {
        int val = readInt(rawDataOff);

        rawDataOff += 4;

        return val;
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != LONG)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readLong(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws GridPortableException {
        long val = readLong(rawDataOff);

        rawDataOff += 8;

        return val;
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != FLOAT)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readFloat(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws GridPortableException {
        float val = readFloat(rawDataOff);

        rawDataOff += 4;

        return val;
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != DOUBLE)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readDouble(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws GridPortableException {
        double val = readDouble(rawDataOff);

        rawDataOff += 8;

        return val;
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != CHAR)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readChar(fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws GridPortableException {
        char val = readChar(rawDataOff);

        rawDataOff += 2;

        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != BOOLEAN)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 && readBoolean(fieldOff);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws GridPortableException {
        boolean val = readBoolean(rawDataOff);

        rawDataOff += 1;

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != STRING)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readString(fieldOff) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws GridPortableException {
        byte[] arr = readByteArray();

        return arr != null ? new String(arr, UTF_8) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != UUID)
            throw new GridPortableException("Invalid flag value: " + flag);

        return fieldOff >= 0 ? readUuid(fieldOff) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws GridPortableException {
        return readBoolean() ? new UUID(readLong(), readLong()) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff < 0)
            return null;

        int off = fieldOff;

        byte flag = readByte(off++);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                return null; // TODO: Handle.

            case OBJ:
                boolean userType = readBoolean(off++);
                int typeId = readInt(off);

                off += 4;

                GridPortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId);

                if (desc == null)
                    throw new GridPortableInvalidClassException("Unknown type ID: " + typeId);

                // Skip hash code and length.
                off += 8;

                int rawDataOff = fieldOff + readInt(off);

                return desc.read(new GridPortableReaderImpl(ctx, arr, fieldOff, rawDataOff));

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject() throws GridPortableException {
        int start = rawDataOff;

        byte flag = readByte();

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                return null; // TODO: Handle.

            case OBJ:
                boolean userType = readBoolean();
                int typeId = readInt();

                GridPortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId);

                if (desc == null)
                    throw new GridPortableInvalidClassException("Unknown type ID: " + typeId);

                // Skip hash code and length.
                rawDataOff += 8;

                int rawDataOff0 = start + readInt();

                return desc.read(new GridPortableReaderImpl(ctx, arr, start, rawDataOff0));

            default:
                throw new GridPortableException("Invalid flag value: " + flag);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != BYTE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readByteArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        byte[] arr = readByteArray(rawDataOff);

        rawDataOff += 4 + arr.length;

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != SHORT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readShortArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        short[] arr = readShortArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 1);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != INT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readIntArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        int[] arr = readIntArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 2);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != LONG_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readLongArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        long[] arr = readLongArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 3);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != FLOAT_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readFloatArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        float[] arr = readFloatArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 2);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != DOUBLE_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readDoubleArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        double[] arr = readDoubleArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 3);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != CHAR_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readCharArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        char[] arr = readCharArray(rawDataOff);

        rawDataOff += 4 + (arr.length << 1);

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != BOOLEAN_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            return readBooleanArray(fieldOff);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        boolean[] arr = readBooleanArray(rawDataOff);

        rawDataOff += 4 + arr.length;

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            byte flag = readByte(fieldOff++);

            if (flag != STRING_ARR)
                throw new GridPortableException("Invalid flag value: " + flag);

            int len = readInt(fieldOff);

            if (len >= 0) {
                String[] strs = new String[len];

                fieldOff += 4;

                for (int i = 0; i < len; i++) {
                    byte[] arr = readByteArray(fieldOff);

                    fieldOff += 4;

                    if (arr != null)
                        fieldOff += arr.length;

                    strs[i] = arr != null ? new String(arr, UTF_8) : null;
                }

                return strs;
            }
            else
                return null;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            String[] arr = new String[len];

            for (int i = 0; i < len; i++)
                arr[i] = readString();

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        byte flag = readByte(fieldOff++);

        if (flag != UUID_ARR)
            throw new GridPortableException("Invalid flag value: " + flag);

        if (fieldOff >= 0) {
            int len = readInt(fieldOff);

            if (len >= 0) {
                UUID[] uuids = new UUID[len];

                fieldOff += 4;

                for (int i = 0; i < len; i++) {
                    UUID uuid = readUuid(fieldOff);

                    fieldOff += (uuid != null ? 17 : 1);

                    uuids[i] = uuid;
                }

                return uuids;
            }
            else
                return null;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            UUID[] arr = new UUID[len];

            for (int i = 0; i < len; i++)
                arr[i] = readUuid();

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            Object[] arr = new Object[len];

            for (int i = 0; i < len; i++)
                arr[i] = readObject();

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws GridPortableException {
        int size = readInt();

        if (size >= 0) {
            Collection<T> col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add((T)readObject());

            return col;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws GridPortableException {
        int size = readInt();

        if (size >= 0) {
            Map<K, V> map = new HashMap<>(size);

            for (int i = 0; i < size; i++)
                map.put((K)readObject(), (V)readObject());

            return map;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public GridPortableRawReader rawReader() {
        return this;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private byte readByte(int off) {
        return PRIM.readByte(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private short readShort(int off) {
        return PRIM.readShort(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private int readInt(int off) {
        return PRIM.readInt(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private long readLong(int off) {
        return PRIM.readLong(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private float readFloat(int off) {
        return PRIM.readFloat(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private double readDouble(int off) {
        return PRIM.readDouble(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private char readChar(int off) {
        return PRIM.readChar(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private boolean readBoolean(int off) {
        return PRIM.readBoolean(arr, off);
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private String readString(int off) {
        byte[] arr = readByteArray(off);

        return arr != null ? new String(arr, UTF_8) : null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private UUID readUuid(int off) {
        if (readBoolean(off)) {
            long most = readLong(off + 1);
            long least = readLong(off + 9);

            return new UUID(most, least);
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private byte[] readByteArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            byte[] arr = new byte[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, BYTE_ARR_OFF, len);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private short[] readShortArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            short[] arr = new short[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, SHORT_ARR_OFF, len << 1);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private int[] readIntArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            int[] arr = new int[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, INT_ARR_OFF, len << 2);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private long[] readLongArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            long[] arr = new long[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, LONG_ARR_OFF, len << 3);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private float[] readFloatArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            float[] arr = new float[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, FLOAT_ARR_OFF, len << 2);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private double[] readDoubleArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            double[] arr = new double[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, DOUBLE_ARR_OFF, len << 3);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private char[] readCharArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            char[] arr = new char[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, CHAR_ARR_OFF, len << 1);

            return arr;
        }
        else
            return null;
    }

    /**
     * @param off Offset.
     * @return Value.
     */
    private boolean[] readBooleanArray(int off) {
        int len = readInt(off);

        if (len >= 0) {
            boolean[] arr = new boolean[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + off + 4, arr, BOOLEAN_ARR_OFF, len);

            return arr;
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

            int off = start + 18;

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
