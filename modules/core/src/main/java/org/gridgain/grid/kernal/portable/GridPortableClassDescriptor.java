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
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.reflect.Modifier.*;

/**
 * Portable class descriptor.
 */
class GridPortableClassDescriptor {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final int COL_TYPE_ID = 100;

    /** */
    private static final int MAP_TYPE_ID = 200;

    /** */
    private static final ConcurrentMap<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    /** */
    static {
        // Boxed primitives.
        CACHE.put(Byte.class, new GridPortableClassDescriptor(Mode.BYTE));
        CACHE.put(Short.class, new GridPortableClassDescriptor(Mode.SHORT));
        CACHE.put(Integer.class, new GridPortableClassDescriptor(Mode.INT));
        CACHE.put(Long.class, new GridPortableClassDescriptor(Mode.LONG));
        CACHE.put(Float.class, new GridPortableClassDescriptor(Mode.FLOAT));
        CACHE.put(Double.class, new GridPortableClassDescriptor(Mode.DOUBLE));
        CACHE.put(Character.class, new GridPortableClassDescriptor(Mode.CHAR));
        CACHE.put(Boolean.class, new GridPortableClassDescriptor(Mode.BOOLEAN));

        // Other objects.
        CACHE.put(String.class, new GridPortableClassDescriptor(Mode.STRING));
        CACHE.put(UUID.class, new GridPortableClassDescriptor(Mode.UUID));

        // Arrays with primitives.
        CACHE.put(byte[].class, new GridPortableClassDescriptor(Mode.BYTE_ARR));
        CACHE.put(short[].class, new GridPortableClassDescriptor(Mode.SHORT_ARR));
        CACHE.put(int[].class, new GridPortableClassDescriptor(Mode.INT_ARR));
        CACHE.put(long[].class, new GridPortableClassDescriptor(Mode.LONG_ARR));
        CACHE.put(float[].class, new GridPortableClassDescriptor(Mode.FLOAT_ARR));
        CACHE.put(double[].class, new GridPortableClassDescriptor(Mode.DOUBLE_ARR));
        CACHE.put(char[].class, new GridPortableClassDescriptor(Mode.CHAR_ARR));
        CACHE.put(boolean[].class, new GridPortableClassDescriptor(Mode.BOOLEAN_ARR));

        // Arrays with boxed primitives.
        CACHE.put(Byte[].class, new GridPortableClassDescriptor(Mode.BYTE_ARR));
        CACHE.put(Short[].class, new GridPortableClassDescriptor(Mode.SHORT_ARR));
        CACHE.put(Integer[].class, new GridPortableClassDescriptor(Mode.INT_ARR));
        CACHE.put(Long[].class, new GridPortableClassDescriptor(Mode.LONG_ARR));
        CACHE.put(Float[].class, new GridPortableClassDescriptor(Mode.FLOAT_ARR));
        CACHE.put(Double[].class, new GridPortableClassDescriptor(Mode.DOUBLE_ARR));
        CACHE.put(Character[].class, new GridPortableClassDescriptor(Mode.CHAR_ARR));
        CACHE.put(Boolean[].class, new GridPortableClassDescriptor(Mode.BOOLEAN_ARR));

        // Other arrays.
        CACHE.put(String[].class, new GridPortableClassDescriptor(Mode.STRING_ARR));
        CACHE.put(UUID[].class, new GridPortableClassDescriptor(Mode.UUID_ARR));
        CACHE.put(Object[].class, new GridPortableClassDescriptor(Mode.OBJ_ARR));
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
    private final Mode mode;

    /** */
    private final int typeId;

    /** */
    private final Constructor<?> cons;

    /** */
    private final Collection<FieldInfo> fields;

    /**
     * @param mode Mode.
     */
    private GridPortableClassDescriptor(Mode mode) {
        assert mode != null;

        assert mode != Mode.COL && mode != Mode.MAP && mode != Mode.PORTABLE && mode != Mode.OBJECT;

        this.mode = mode;

        typeId = 0;
        cls = null;
        cons = null;
        fields = null;
    }

    /**
     * @param cls Class.
     */
    private GridPortableClassDescriptor(Class<?> cls) throws GridPortableException {
        assert cls != null;

        this.cls = cls;

        if (Collection.class.isAssignableFrom(cls)) {
            mode = Mode.COL;
            typeId = COL_TYPE_ID;
            cons = null;
            fields = null;
        }
        else if (Map.class.isAssignableFrom(cls)) {
            mode = Mode.MAP;
            typeId = MAP_TYPE_ID;
            cons = null;
            fields = null;
        }
        else if (GridPortable.class.isAssignableFrom(cls)) {
            mode = Mode.PORTABLE;
            typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config
            cons = constructor(cls);
            fields = null;
        }
        else {
            mode = Mode.OBJECT;
            typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config
            cons = constructor(cls);

            fields = new ArrayList<>();

            Collection<String> names = new HashSet<>();
            Collection<Integer> ids = new HashSet<>();

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        String name = f.getName();

                        if (!names.add(name))
                            throw new GridPortableException("Duplicate field name: " + name);

                        int id = name.hashCode(); // TODO: take from mapper.

                        if (!ids.add(id))
                            throw new GridPortableException("Duplicate field ID: " + name); // TODO: proper message

                        fields.add(new FieldInfo(f, id));
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

        writer.doWriteByte(mode.flag);

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
                writer.doWriteInt(typeId);
                writer.doWriteInt(obj.hashCode());

                // Length + raw data offset.
                writer.reserve(8);

                ((GridPortable)obj).writePortable(writer);

                writer.writeLength();

                break;

            case OBJECT:
                writer.doWriteInt(typeId);
                writer.doWriteInt(obj.hashCode());

                // Length + raw data offset.
                writer.reserve(8);

                for (FieldInfo info : fields)
                    info.write(obj, writer);

                writer.writeLength();

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
                GridPortable portable = newInstance();

                portable.readPortable(reader);

                return portable;

            case OBJECT:
                GridPortable obj = newInstance();

                for (FieldInfo info : fields) {
                    Field f = info.field;

                    Object val = reader.readObject(f.getName());

                    try {
                        f.set(obj, val);
                    }
                    catch (IllegalAccessException e) {
                        throw new GridPortableException("Failed to set value for field: " + f, e);
                    }
                }

                return obj;

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }
    }

    /**
     * @return Instance.
     * @throws GridPortableException In case of error.
     */
    private <T> T newInstance() throws GridPortableException {
        try {
            return cons != null ? (T)cons.newInstance() : (T)UNSAFE.allocateInstance(cls);
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new GridPortableException("Failed to instantiate instance: " + cls, e);
        }
    }

    /**
     * @param cls Class.
     * @return Constructor.
     */
    @Nullable private static Constructor<?> constructor(Class<?> cls) {
        try {
            Constructor<?> cons = cls.getConstructor();

            cons.setAccessible(true);

            return cons;
        }
        catch (NoSuchMethodException ignored) {
            return null;
        }
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final int id;

        /**
         * @param field Field.
         * @param id Field ID.
         */
        private FieldInfo(Field field, int id) {
            assert field != null;

            this.field = field;
            this.id = id;
        }

        /**
         * @param obj Object.
         * @param writer Writer.
         * @throws GridPortableException In case of error.
         */
        public void write(Object obj, GridPortableWriterImpl writer) throws GridPortableException {
            assert obj != null;
            assert writer != null;

            writer.doWriteInt(id);

            Object val;

            try {
                val = field.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new GridPortableException("Failed to get value for field: " + field, e);
            }

            int lenPos = writer.reserveAndMark(4);

            writer.doWriteObject(val);

            writer.writeDelta(lenPos);
        }
    }

    /** */
    private enum Mode {
        /** */
        BYTE(GridPortableMarshaller.BYTE),

        /** */
        SHORT(GridPortableMarshaller.SHORT),

        /** */
        INT(GridPortableMarshaller.INT),

        /** */
        LONG(GridPortableMarshaller.LONG),

        /** */
        FLOAT(GridPortableMarshaller.FLOAT),

        /** */
        DOUBLE(GridPortableMarshaller.DOUBLE),

        /** */
        CHAR(GridPortableMarshaller.CHAR),

        /** */
        BOOLEAN(GridPortableMarshaller.BOOLEAN),

        /** */
        STRING(GridPortableMarshaller.STRING),

        /** */
        UUID(GridPortableMarshaller.UUID),

        /** */
        BYTE_ARR(GridPortableMarshaller.BYTE_ARR),

        /** */
        SHORT_ARR(GridPortableMarshaller.SHORT_ARR),

        /** */
        INT_ARR(GridPortableMarshaller.INT_ARR),

        /** */
        LONG_ARR(GridPortableMarshaller.LONG_ARR),

        /** */
        FLOAT_ARR(GridPortableMarshaller.FLOAT_ARR),

        /** */
        DOUBLE_ARR(GridPortableMarshaller.DOUBLE_ARR),

        /** */
        CHAR_ARR(GridPortableMarshaller.CHAR_ARR),

        /** */
        BOOLEAN_ARR(GridPortableMarshaller.BOOLEAN_ARR),

        /** */
        STRING_ARR(GridPortableMarshaller.STRING_ARR),

        /** */
        UUID_ARR(GridPortableMarshaller.UUID_ARR),

        /** */
        OBJ_ARR(GridPortableMarshaller.OBJ_ARR),

        /** */
        COL(GridPortableMarshaller.COL),

        /** */
        MAP(GridPortableMarshaller.MAP),

        /** */
        PORTABLE(GridPortableMarshaller.OBJ),

        /** */
        OBJECT(GridPortableMarshaller.OBJ);

        /** */
        private final byte flag;

        /**
         * @param flag Flag.
         */
        Mode(byte flag) {
            this.flag = flag;
        }
    }
}
