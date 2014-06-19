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
    private static final boolean useNames = false; // TODO: take from config

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

        switch (mode) {
            case BYTE:
                writer.doWriteByte((byte)obj);

                break;

            case SHORT:
                writer.doWriteShort((short)obj);

                break;

            case INT:
                writer.doWriteInt((int)obj);

                break;

            case LONG:
                writer.doWriteLong((long)obj);

                break;

            case FLOAT:
                writer.doWriteFloat((float)obj);

                break;

            case DOUBLE:
                writer.doWriteDouble((double)obj);

                break;

            case CHAR:
                writer.doWriteChar((char)obj);

                break;

            case BOOLEAN:
                writer.doWriteBoolean((boolean)obj);

                break;

            case STRING:
                writer.doWriteString((String)obj);

                break;

            case UUID:
                writer.doWriteUuid((UUID)obj);

                break;

            case BYTE_ARR:
                writer.doWriteByteArray((byte[])obj);

                break;

            case SHORT_ARR:
                writer.doWriteShortArray((short[])obj);

                break;

            case INT_ARR:
                writer.doWriteIntArray((int[])obj);

                break;

            case LONG_ARR:
                writer.doWriteLongArray((long[])obj);

                break;

            case FLOAT_ARR:
                writer.doWriteFloatArray((float[])obj);

                break;

            case DOUBLE_ARR:
                writer.doWriteDoubleArray((double[])obj);

                break;

            case CHAR_ARR:
                writer.doWriteCharArray((char[])obj);

                break;

            case BOOLEAN_ARR:
                writer.doWriteBooleanArray((boolean[])obj);

                break;

            case STRING_ARR:
                writer.doWriteStringArray((String[])obj);

                break;

            case UUID_ARR:
                writer.doWriteUuidArray((UUID[])obj);

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

                writer.doWriteBoolean(useNames);

                ((GridPortable)obj).writePortable(writer);

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            case OBJECT:
                writer.doWriteInt(obj.hashCode());

                // Length + raw data offset.
                writer.reserve(8);

                writer.doWriteBoolean(useNames);

                for (FieldInfo info : fields)
                    info.write(obj, writer);

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

    /**
     * @param field Field.
     * @return Field type.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static FieldType fieldType(Field field) {
        Class<?> cls = field.getType();

        FieldType type;

        if (cls == byte.class)
            type = FieldType.BYTE;
        else if (cls == short.class)
            type = FieldType.SHORT;
        else if (cls == int.class)
            type = FieldType.INT;
        else if (cls == long.class)
            type = FieldType.LONG;
        else if (cls == float.class)
            type = FieldType.FLOAT;
        else if (cls == double.class)
            type = FieldType.DOUBLE;
        else if (cls == char.class)
            type = FieldType.CHAR;
        else if (cls == boolean.class)
            type = FieldType.BOOLEAN;
        else
            type = FieldType.OTHER;

        return type;
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final FieldType type;

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

            type = fieldType(field);

            name = null;
        }

        /**
         * @param field Field.
         * @param name Field name.
         */
        private FieldInfo(Field field, byte[] name) {
            assert field != null;
            assert name != null;

            this.field = field;
            this.name = name;

            type = fieldType(field);

            id = 0;
        }

        /**
         * @param obj Object.
         * @param writer Writer.
         * @throws GridPortableException In case of error.
         */
        public void write(Object obj, GridPortableWriterImpl writer) throws GridPortableException {
            assert obj != null;
            assert writer != null;

            if (name != null)
                writer.doWriteByteArray(name);
            else
                writer.doWriteInt(id);

            Object val;

            try {
                val = field.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new GridPortableException("Failed to get value for field: " + field, e);
            }

            switch (type) {
                case BYTE:
                    writer.doWriteInt(1);
                    writer.doWriteByte((byte)val);

                    break;

                case SHORT:
                    writer.doWriteInt(2);
                    writer.doWriteShort((short)val);

                    break;

                case INT:
                    writer.doWriteInt(4);
                    writer.doWriteInt((int)val);

                    break;

                case LONG:
                    writer.doWriteInt(8);
                    writer.doWriteLong((long)val);

                    break;

                case FLOAT:
                    writer.doWriteInt(4);
                    writer.doWriteFloat((float)val);

                    break;

                case DOUBLE:
                    writer.doWriteInt(8);
                    writer.doWriteDouble((double)val);

                    break;

                case CHAR:
                    writer.doWriteInt(2);
                    writer.doWriteChar((char)val);

                    break;

                case BOOLEAN:
                    writer.doWriteInt(1);
                    writer.doWriteBoolean((boolean)val);

                    break;

                case OTHER:
                    int lenPos = writer.reserveAndMark(4);

                    writer.doWriteObject(val);

                    writer.writeDelta(lenPos);

                    break;

                default:
                    assert false : "Invalid field type: " + type;
            }
        }
    }

    /** */
    private enum FieldType {
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
        OTHER
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
