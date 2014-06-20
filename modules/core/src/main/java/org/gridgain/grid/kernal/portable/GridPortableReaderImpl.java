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
    private final Map<Integer, Integer> fieldsOffs = new HashMap<>();

    /** */
    private final byte[] arr;

    /** */
    private int off;

    /** */
    private int rawOff;

    /**
     * @param arr Array.
     */
    GridPortableReaderImpl(byte[] arr) {
        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readByte(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws GridPortableException {
        byte val = PRIM.readByte(arr, rawOff);

        rawOff++;

        return val;
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readShort(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws GridPortableException {
        short val = PRIM.readShort(arr, rawOff);

        rawOff += 2;

        return val;
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readInt(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws GridPortableException {
        int val = PRIM.readInt(arr, rawOff);

        rawOff += 4;

        return val;
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readLong(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws GridPortableException {
        long val = PRIM.readLong(arr, rawOff);

        rawOff += 8;

        return val;
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readFloat(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws GridPortableException {
        float val = PRIM.readFloat(arr, rawOff);

        rawOff += 4;

        return val;
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readDouble(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws GridPortableException {
        double val = PRIM.readDouble(arr, rawOff);

        rawOff += 8;

        return val;
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 ? PRIM.readChar(arr, fieldOff) : 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws GridPortableException {
        char val = PRIM.readChar(arr, rawOff);

        rawOff += 2;

        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        return fieldOff >= 0 && PRIM.readBoolean(arr, fieldOff);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws GridPortableException {
        boolean val = PRIM.readBoolean(arr, rawOff);

        rawOff += 1;

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            if (len < 0)
                return null;

            byte[] arr = new byte[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff, arr, BYTE_ARR_OFF, len);

            return new String(arr, UTF_8);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws GridPortableException {
        byte[] arr = readByteArray();

        return arr != null ? new String(arr, UTF_8) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            if (PRIM.readBoolean(arr, fieldOff)) {
                long most = PRIM.readLong(arr, fieldOff + 1);
                long least = PRIM.readLong(arr, fieldOff + 9);

                return new UUID(most, least);
            }
            else
                return null;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws GridPortableException {
        return readBoolean() ? new UUID(readLong(), readLong()) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObject() throws GridPortableException {
//        byte flag = PRIM.readByte(arr, off++);
//
//        switch (flag) {
//            case NULL:
//                return null;
//
//            case HANDLE:
//                return null; // TODO: Handle.
//
//            case BYTE:
//                byte b = readByte();
//
//            case BYTE:
//                return readByte();
//
//            case BYTE:
//                return readByte();
//        }
//
//        int typeId = PRIM.readInt(arr, off);
//        int hashCode = PRIM.readInt(arr, off + 4);
//
//        // TODO: Length is skipped!
//
//        rawOff = PRIM.readInt(arr, off + 13);
//
//        off += 17;

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            byte[] arr = new byte[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, BYTE_ARR_OFF, len);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            byte[] arr = new byte[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, BYTE_ARR_OFF, len);

            rawOff += len;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            short[] arr = new short[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, SHORT_ARR_OFF, len << 1);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            short[] arr = new short[len];

            int bytes = len << 1;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, SHORT_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            int[] arr = new int[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, INT_ARR_OFF, len << 2);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            int[] arr = new int[len];

            int bytes = len << 2;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, INT_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            long[] arr = new long[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, LONG_ARR_OFF, len << 3);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            long[] arr = new long[len];

            int bytes = len << 3;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, LONG_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            float[] arr = new float[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, FLOAT_ARR_OFF, len << 2);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            float[] arr = new float[len];

            int bytes = len << 2;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, FLOAT_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            double[] arr = new double[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, DOUBLE_ARR_OFF, len << 3);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            double[] arr = new double[len];

            int bytes = len << 3;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, DOUBLE_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            char[] arr = new char[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, CHAR_ARR_OFF, len << 1);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            char[] arr = new char[len];

            int bytes = len << 1;

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, CHAR_ARR_OFF, bytes);

            rawOff += bytes;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            boolean[] arr = new boolean[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff + 4, arr, BYTE_ARR_OFF, len);

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        int len = readInt();

        if (len >= 0) {
            boolean[] arr = new boolean[len];

            UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + rawOff, arr, BOOLEAN_ARR_OFF, len);

            rawOff += len;

            return arr;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        int fieldOff = fieldOffset(fieldName);

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            fieldOff += 4;

            if (len < 0)
                return null;

            String[] strs = new String[len];

            for (int i = 0; i < len; i++) {
                int strLen = PRIM.readInt(arr, fieldOff);

                fieldOff += 4;

                if (strLen >= 0) {
                    byte[] arr = new byte[strLen];

                    UNSAFE.copyMemory(this.arr, BYTE_ARR_OFF + fieldOff, arr, BYTE_ARR_OFF, strLen);

                    fieldOff += strLen;

                    strs[i] = new String(arr, UTF_8);
                }
            }

            return strs;
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

        if (fieldOff >= 0) {
            int len = PRIM.readInt(arr, fieldOff);

            fieldOff += 4;

            if (len < 0)
                return null;

            UUID[] uuids = new UUID[len];

            for (int i = 0; i < len; i++) {
                if (PRIM.readBoolean(arr, fieldOff)) {
                    long most = PRIM.readLong(arr, fieldOff + 1);
                    long least = PRIM.readLong(arr, fieldOff + 9);

                    uuids[i] = new UUID(most, least);
                }
            }

            return uuids;
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
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldOffset(String name) {
        assert name != null;

        int hash = name.hashCode();

        Integer fieldOff = fieldsOffs.get(hash);

        if (fieldOff == null) {
            while (true) {
                if (off >= arr.length)
                    return -1;

                int hash0 = PRIM.readInt(arr, off);
                int len = PRIM.readInt(arr, off);

                off += 8;

                fieldsOffs.put(hash0, fieldOff = off);

                off += len;

                if (hash0 == hash)
                    break;
            }
        }

        return fieldOff;
    }
}
