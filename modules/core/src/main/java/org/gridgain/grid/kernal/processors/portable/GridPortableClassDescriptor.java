/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

import static java.lang.reflect.Modifier.*;

/**
 * Portable class descriptor.
 */
public class GridPortableClassDescriptor {
    /** */
    private final Class<?> cls;

    /** */
    private final GridPortableSerializer serializer;

    /** */
    private final Mode mode;

    /** */
    private final boolean userType;

    /** */
    private final int typeId;

    /** */
    private final Constructor<?> cons;

    /** */
    private final Collection<FieldInfo> fields;

    /**
     * @param cls Class.
     */
    public GridPortableClassDescriptor(Class<?> cls, int typeId, @Nullable GridPortableIdMapper idMapper,
        @Nullable GridPortableSerializer serializer) throws GridPortableException {
        assert cls != null;

        this.cls = cls;
        this.typeId = typeId;
        this.serializer = serializer;

        if (cls == Byte.class) {
            mode = Mode.BYTE;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Short.class) {
            mode = Mode.SHORT;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Integer.class) {
            mode = Mode.INT;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Long.class) {
            mode = Mode.LONG;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Float.class) {
            mode = Mode.FLOAT;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Double.class) {
            mode = Mode.DOUBLE;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Character.class) {
            mode = Mode.CHAR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Boolean.class) {
            mode = Mode.BOOLEAN;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == String.class) {
            mode = Mode.STRING;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == UUID.class) {
            mode = Mode.UUID;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == byte[].class) {
            mode = Mode.BYTE_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == short[].class) {
            mode = Mode.SHORT_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == int[].class) {
            mode = Mode.INT_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == long[].class) {
            mode = Mode.LONG_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == float[].class) {
            mode = Mode.FLOAT_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == double[].class) {
            mode = Mode.DOUBLE_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == char[].class) {
            mode = Mode.CHAR_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == boolean[].class) {
            mode = Mode.BOOLEAN_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == String[].class) {
            mode = Mode.STRING_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == UUID[].class) {
            mode = Mode.UUID_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (cls == Object[].class) {
            mode = Mode.OBJ_ARR;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (Collection.class.isAssignableFrom(cls)) {
            mode = Mode.COL;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (Map.class.isAssignableFrom(cls)) {
            mode = Mode.MAP;
            userType = false;
            cons = null;
            fields = null;
        }
        else if (serializer != null || GridPortable.class.isAssignableFrom(cls)) {
            mode = Mode.PORTABLE;
            userType = true;
            cons = constructor(cls);
            fields = null;
        }
        else {
            mode = Mode.OBJECT;
            userType = true;
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

                        Integer fieldId = null;

                        GridPortableId idAnn = f.getAnnotation(GridPortableId.class);

                        if (idAnn != null)
                            fieldId = idAnn.id();
                        else if (idMapper != null)
                            fieldId = idMapper.fieldId(typeId, f.getName());

                        if (fieldId == null)
                            fieldId = f.getName().hashCode();

                        if (!ids.add(fieldId))
                            throw new GridPortableException("Duplicate field ID: " + name);

                        fields.add(new FieldInfo(f, fieldId));
                    }
                }
            }
        }
    }

    /**
     * @return Type ID.
     */
    int typeId() {
        return typeId;
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
        writer.doWriteInt(obj.hashCode());

        // Length.
        writer.reserve(4);

        // Default raw offset (equal to header length).
        writer.doWriteInt(18);

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
                if (serializer != null)
                    serializer.writePortable(obj, writer);
                else
                    ((GridPortable)obj).writePortable(writer);

                break;

            case OBJECT:
                for (FieldInfo info : fields)
                    info.write(obj, writer);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }

        writer.writeLength();
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
                return reader.readIntArray();

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
                Object portable = newInstance();

                if (serializer != null)
                    serializer.readPortable(portable, reader);
                else
                    ((GridPortable)portable).readPortable(reader);

                return portable;

            case OBJECT:
                Object obj = newInstance();

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
    private Object newInstance() throws GridPortableException {
        assert cons != null;

        try {
            return cons.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new GridPortableException("Failed to instantiate instance: " + cls, e);
        }
    }

    /**
     * @param cls Class.
     * @return Constructor.
     * @throws GridPortableException If constructor doesn't exist.
     */
    @Nullable private static Constructor<?> constructor(Class<?> cls) throws GridPortableException {
        assert cls != null;

        try {
            Constructor<?> cons = cls.getDeclaredConstructor();

            cons.setAccessible(true);

            return cons;
        }
        catch (NoSuchMethodException e) {
            throw new GridPortableException("Class doesn't have default constructor: " + cls.getName(), e);
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
