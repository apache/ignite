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
import org.jdk8.backport.*;
import sun.misc.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.reflect.Modifier.*;
import static java.nio.charset.StandardCharsets.*;

/**
 * Portable class descriptor.
 */
class GridPortableClassDescriptor {
    /** */
    protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final int TOTAL_LEN_POS = 10;

    /** */
    private static final int COL_TYPE_ID = 100;

    /** */
    private static final int MAP_TYPE_ID = 200;

    /** */
    private static final ConcurrentMap<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    /** */
    static {
        // Boxed primitives.
        CACHE.put(Byte.class, new GridPortableClassDescriptor(Mode.BYTE, 1));
        CACHE.put(Short.class, new GridPortableClassDescriptor(Mode.SHORT, 2));
        CACHE.put(Integer.class, new GridPortableClassDescriptor(Mode.INT, 3));
        CACHE.put(Long.class, new GridPortableClassDescriptor(Mode.LONG, 4));
        CACHE.put(Float.class, new GridPortableClassDescriptor(Mode.FLOAT, 5));
        CACHE.put(Double.class, new GridPortableClassDescriptor(Mode.DOUBLE, 6));
        CACHE.put(Character.class, new GridPortableClassDescriptor(Mode.CHAR, 7));
        CACHE.put(Boolean.class, new GridPortableClassDescriptor(Mode.BOOLEAN, 8));

        // Other objects.
        CACHE.put(String.class, new GridPortableClassDescriptor(Mode.STRING, 9));
        CACHE.put(UUID.class, new GridPortableClassDescriptor(Mode.UUID, 10));

        // Arrays with primitives.
        CACHE.put(byte[].class, new GridPortableClassDescriptor(Mode.BYTE_ARR, 11));
        CACHE.put(short[].class, new GridPortableClassDescriptor(Mode.SHORT_ARR, 12));
        CACHE.put(int[].class, new GridPortableClassDescriptor(Mode.INT_ARR, 13));
        CACHE.put(long[].class, new GridPortableClassDescriptor(Mode.LONG_ARR, 14));
        CACHE.put(float[].class, new GridPortableClassDescriptor(Mode.FLOAT_ARR, 15));
        CACHE.put(double[].class, new GridPortableClassDescriptor(Mode.DOUBLE_ARR, 16));
        CACHE.put(char[].class, new GridPortableClassDescriptor(Mode.CHAR_ARR, 17));
        CACHE.put(boolean[].class, new GridPortableClassDescriptor(Mode.BOOLEAN_ARR, 18));

        // Arrays with boxed primitives.
        CACHE.put(Byte[].class, new GridPortableClassDescriptor(Mode.BYTE_ARR, 11));
        CACHE.put(Short[].class, new GridPortableClassDescriptor(Mode.SHORT_ARR, 12));
        CACHE.put(Integer[].class, new GridPortableClassDescriptor(Mode.INT_ARR, 13));
        CACHE.put(Long[].class, new GridPortableClassDescriptor(Mode.LONG_ARR, 14));
        CACHE.put(Float[].class, new GridPortableClassDescriptor(Mode.FLOAT_ARR, 15));
        CACHE.put(Double[].class, new GridPortableClassDescriptor(Mode.DOUBLE_ARR, 16));
        CACHE.put(Character[].class, new GridPortableClassDescriptor(Mode.CHAR_ARR, 17));
        CACHE.put(Boolean[].class, new GridPortableClassDescriptor(Mode.BOOLEAN_ARR, 18));

        // Other arrays.
        CACHE.put(String[].class, new GridPortableClassDescriptor(Mode.STRING_ARR, 19));
        CACHE.put(UUID[].class, new GridPortableClassDescriptor(Mode.UUID_ARR, 20));
        CACHE.put(Object[].class, new GridPortableClassDescriptor(Mode.OBJ_ARR, 21));
    }

    /**
     * @param cls Class.
     * @return Class descriptor.
     * @throws GridPortableException In case of error.
     */
    static GridPortableClassDescriptor get(Class<?> cls) throws GridPortableException {
        assert cls != null;

        GridPortableClassDescriptor desc = CACHE.get(cls);

        if (desc == null) {
            GridPortableClassDescriptor old = CACHE.putIfAbsent(cls, desc = new GridPortableClassDescriptor(cls));

            if (old != null)
                desc = old;
        }

        return desc;
    }

    /** */
    private final Class<?> cls;

    /** */
    private final boolean userType;

    /** */
    private final Mode mode;

    /** */
    private final int typeId;

    /** */
    private final boolean useNames = false; // TODO: take from config

    /** */
    private Collection<FieldInfo> fields;

    /**
     * @param mode Mode.
     * @param typeId Type ID.
     */
    private GridPortableClassDescriptor(Mode mode, int typeId) {
        assert mode != null;

        assert mode != Mode.COL && mode != Mode.MAP && mode != Mode.PORTABLE && mode != Mode.OBJECT;

        cls = null;
        userType = false;

        this.mode = mode;
        this.typeId = typeId;
    }

    /**
     * @param cls Class.
     */
    private GridPortableClassDescriptor(Class<?> cls) throws GridPortableException {
        assert cls != null;

        this.cls = cls;

        if (Collection.class.isAssignableFrom(cls)) {
            userType = false;
            mode = Mode.COL;
            typeId = COL_TYPE_ID;
        }
        else if (Map.class.isAssignableFrom(cls)) {
            userType = false;
            mode = Mode.MAP;
            typeId = MAP_TYPE_ID;
        }
        else if (GridPortable.class.isAssignableFrom(cls)) {
            userType = true;
            mode = Mode.PORTABLE;
            typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config
        }
        else {
            userType = true;
            mode = Mode.OBJECT;
            typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config

            fields = new ArrayList<>();

            Collection<String> names = new HashSet<>();
            Collection<Integer> ids = useNames ? null : new HashSet<Integer>();

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        String name = f.getName();

                        if (!names.add(name))
                            throw new GridPortableException("Duplicate field name: " + name);

                        if (useNames)
                            fields.add(new FieldInfo(f, name.getBytes(UTF_8)));
                        else {
                            int id = name.hashCode();

                            if (!ids.add(id))
                                throw new GridPortableException("Duplicate field ID: " + name); // TODO: proper message

                            fields.add(new FieldInfo(f, id));
                        }
                    }
                }
            }
        }
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    void write(Object obj, GridPortableWriterImpl writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        writer.doWriteBoolean(userType);
        writer.doWriteInt(typeId);

        write0(obj, writer, mode, false);
    }

    private void write0(Object obj, GridPortableWriterImpl writer, Mode mode, boolean writeLen)
        throws GridPortableException {
        switch (mode) {
            case BYTE:
                if (writeLen)
                    writer.doWriteInt(1);

                writer.doWriteByte((byte)obj);

                break;

            case SHORT:
                if (writeLen)
                    writer.doWriteInt(2);

                writer.doWriteShort((short)obj);

                break;

            case INT:
                if (writeLen)
                    writer.doWriteInt(4);

                writer.doWriteInt((int)obj);

                break;

            case LONG:
                if (writeLen)
                    writer.doWriteInt(8);

                writer.doWriteLong((long)obj);

                break;

            case FLOAT:
                if (writeLen)
                    writer.doWriteInt(4);

                writer.doWriteFloat((float)obj);

                break;

            case DOUBLE:
                if (writeLen)
                    writer.doWriteInt(8);

                writer.doWriteDouble((double)obj);

                break;

            case CHAR:
                if (writeLen)
                    writer.doWriteInt(2);

                writer.doWriteChar((char)obj);

                break;

            case BOOLEAN:
                if (writeLen)
                    writer.doWriteInt(1);

                writer.doWriteBoolean((boolean)obj);

                break;

            case STRING:
                byte[] strbyteArr = null;
                int strLen = 4;

                if (obj != null) {
                    strbyteArr = ((String)obj).getBytes(UTF_8);
                    strLen += strbyteArr.length;
                }

                if (writeLen)
                    writer.writeInt(strLen);

                writer.doWriteByteArray(strbyteArr);

                break;

            case UUID:
                if (writeLen)
                    writer.doWriteInt(obj != null ? 17 : 1);

                writer.doWriteUuid((UUID)obj);

                break;

            case BYTE_ARR:
                byte[] byteArr = (byte[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + byteArr.length);

                writer.doWriteByteArray(byteArr);

                break;

            case SHORT_ARR:
                short[] shortArr = (short[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (shortArr != null ? shortArr.length << 1 : 0));

                writer.doWriteShortArray(shortArr);

                break;

            case INT_ARR:
                int[] intArr = (int[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (intArr != null ? intArr.length << 2 : 0));

                writer.doWriteIntArray(intArr);

                break;

            case LONG_ARR:
                long[] longArr = (long[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (longArr != null ? longArr.length << 1 : 0));

                writer.doWriteLongArray(longArr);

                break;

            case FLOAT_ARR:
                float[] floatArr = (float[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (floatArr != null ? floatArr.length << 1 : 0));

                writer.doWriteFloatArray(floatArr);

                break;

            case DOUBLE_ARR:
                double[] doubleArr = (double[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (doubleArr != null ? doubleArr.length << 1 : 0));

                writer.doWriteDoubleArray(doubleArr);

                break;

            case CHAR_ARR:
                char[] charArr = (char[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (charArr != null ? charArr.length << 1 : 0));

                writer.doWriteCharArray(charArr);

                break;

            case BOOLEAN_ARR:
                boolean[] booleanArr = (boolean[])obj;

                if (writeLen)
                    writer.doWriteInt(4 + (booleanArr != null ? booleanArr.length << 1 : 0));

                writer.doWriteBooleanArray(booleanArr);

                break;

            case STRING_ARR:
                // TODO

//                String[] strArr = (String[])obj;
//
//                byte[][] strbyteArrs = new byte[strArr.length][];
//
//                int strArrLen = 4;
//
//                if (writeLen)
//                    writer.doWriteInt(4 + (shortArr != null ? shortArr.length << 1 : 0));
//
//                writer.doWriteStringArray((String[])obj);

                break;

            case UUID_ARR:
                // TODO

//                UUID[] uuidArr = (UUID[])obj;
//
//                if (writeLen)
//                    writer.doWriteInt();
//
//                writer.doWriteUuidArray((UUID[])obj);

                break;

            case OBJ_ARR:
                writer.doWriteObjectArray((Object[])obj);

                break;

            case COL:
                writer.doWriteCollection((Collection<?>)obj);

                break;

            case MAP:
                writer.doWriteMap((Map<?, ?>)obj);

                break;

            case PORTABLE:
                writer.doWriteInt(obj.hashCode());

                // Length + raw data offset.
                writer.reserve(8);

                // Header handles are not supported for GridPortable.
                writer.doWriteInt(-1);

                GridPortableWriterImpl writer0 = new GridPortableWriterImpl(writer);

                ((GridPortable)obj).writePortable(writer0);

                writer0.flush();

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            case OBJECT:
                writer.doWriteInt(obj.hashCode());

                // Length + raw data offset.
                writer.reserve(8);

                writer.doWriteBoolean(useNames);

                for (FieldInfo info : fields) {
                    if (useNames)
                        writer.doWriteByteArray(info.name);
                    else
                        writer.doWriteInt(info.id);

                    // Field length.
                    writer.reserve(4);

                    try {
                        write0(info.field.get(obj), writer, info.mode, true);
                    }
                    catch (IllegalAccessException e) {
                        throw new GridPortableException("Failed to get value for field: " + info.field, e);
                    }
                }

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /**
     * @param reader Reader.
     * @return Object.
     */
    Object read(GridPortableReaderImpl reader) throws GridPortableException {
        assert reader != null;

        switch (mode) {
            case BYTE:
                return reader.readByte();

            case SHORT:
                return reader.readShort();

            case INT:
                return reader.readInt();

            case LONG:
                return reader.readLong();

            case FLOAT:
                return reader.readFloat();

            case DOUBLE:
                return reader.readDouble();

            case CHAR:
                return reader.readChar();

            case BOOLEAN:
                return reader.readBoolean();

            case STRING:
                return reader.readString();

            case UUID:
                return reader.readUuid();

            case BYTE_ARR:
                return reader.readByteArray();

            case SHORT_ARR:
                return reader.readShortArray();

            case INT_ARR:
                return reader.readShortArray();

            case LONG_ARR:
                return reader.readLongArray();

            case FLOAT_ARR:
                return reader.readFloatArray();

            case DOUBLE_ARR:
                return reader.readDoubleArray();

            case CHAR_ARR:
                return reader.readCharArray();

            case BOOLEAN_ARR:
                return reader.readBooleanArray();

            case STRING_ARR:
                return reader.readStringArray();

            case UUID_ARR:
                return reader.readUuidArray();

            case OBJ_ARR:
                return reader.readObjectArray();

            case COL:
                return reader.readCollection();

            case MAP:
                return reader.readMap();

            case PORTABLE:
                GridPortable portableEx = newInstance(cls);

                portableEx.readPortable(reader);

                return portableEx;

            case OBJECT:
                GridPortable portable = newInstance(cls);

                for (FieldInfo info : fields) {
                    Field f = info.field;

                    Object val = reader.readObject(f.getName());

                    try {
                        f.set(portable, val);
                    }
                    catch (IllegalAccessException e) {
                        throw new GridPortableException("Failed to set value for field: " + f, e);
                    }
                }

                return portable;

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }
    }

    /**
     * @param cls Class.
     * @return Instance.
     * @throws GridPortableException In case of error.
     */
    private <T> T newInstance(Class<?> cls) throws GridPortableException {
        try {
            return (T)UNSAFE.allocateInstance(cls);
        }
        catch (InstantiationException e) {
            throw new GridPortableException("Failed to instantiate instance: " + cls, e);
        }
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final Mode mode;

        /** */
        private final int id;

        /** */
        private final byte[] name;

        /**
         * @param field Field.
         * @param id Field ID.
         */
        private FieldInfo(Field field, int id) {
            assert field != null;

            this.field = field;
            this.id = id;

            name = null;
        }

        /**
         * @param field Field.
         * @param name Field name.
         */
        private FieldInfo(Field field, byte[] name) {
            this.field = field;
            this.name = name;

            id = 0;
        }
    }

    /** */
    private enum Mode {
        /** */
        BYTE,

        /** */
        SHORT,

        /** */
        INT,

        /** */
        LONG,

        /** */
        FLOAT,

        /** */
        DOUBLE,

        /** */
        CHAR,

        /** */
        BOOLEAN,

        /** */
        STRING,

        /** */
        UUID,

        /** */
        BYTE_ARR,

        /** */
        SHORT_ARR,

        /** */
        INT_ARR,

        /** */
        LONG_ARR,

        /** */
        FLOAT_ARR,

        /** */
        DOUBLE_ARR,

        /** */
        CHAR_ARR,

        /** */
        BOOLEAN_ARR,

        /** */
        STRING_ARR,

        /** */
        UUID_ARR,

        /** */
        OBJ_ARR,

        /** */
        COL,

        /** */
        MAP,

        /** */
        PORTABLE,

        /** */
        OBJECT
    }
}
