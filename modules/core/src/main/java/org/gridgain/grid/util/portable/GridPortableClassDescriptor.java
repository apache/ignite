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
     * @param userType User type flag.
     * @param typeId Type ID.
     * @param idMapper ID mapper.
     * @param serializer Serializer.
     * @throws GridPortableException In case of error.
     */
    GridPortableClassDescriptor(Class<?> cls, boolean userType, int typeId, @Nullable GridPortableIdMapper idMapper,
        @Nullable GridPortableSerializer serializer) throws GridPortableException {
        assert cls != null;

        this.cls = cls;
        this.userType = userType;
        this.typeId = typeId;
        this.serializer = serializer;

        mode = serializer != null ? Mode.PORTABLE : mode(cls);

        switch (mode) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case BOOLEAN:
            case STRING:
            case UUID:
            case DATE:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case OBJ_ARR:
            case COL:
            case MAP:
                cons = null;
                fields = null;

                break;

            case PORTABLE:
                cons = constructor(cls);
                fields = null;

                break;

            case OBJECT:
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

                break;

            default:
                // Should never happen.
                throw new GridPortableException("Invalid mode: " + mode);
        }
    }

    /**
     * @return User type flag.
     */
    public boolean userType() {
        return userType;
    }

    /**
     * Gets portable type name.
     *
     * @return Type name.
     */
    public String name() {
        return cls.getName();
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

            case DATE:
                writer.doWriteDate((Date)obj);

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

            case DATE_ARR:
                writer.doWriteDateArray((Date[])obj);

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

                writer.writeRawOffsetIfNeeded();

                break;

            case OBJECT:
                for (FieldInfo info : fields)
                    info.write(obj, writer);

                writer.writeRawOffsetIfNeeded();

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

            case DATE:
                return reader.readDate();

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

            case DATE_ARR:
                return reader.readDateArray();

            case OBJ_ARR:
                return reader.readObjectArray();

            case COL:
                return reader.readCollection();

            case MAP:
                return reader.readMap();

            case PORTABLE:
                Object portable = newInstance();

                reader.setHandler(portable);

                if (serializer != null)
                    serializer.readPortable(portable, reader);
                else
                    ((GridPortable)portable).readPortable(reader);

                return portable;

            case OBJECT:
                Object obj = newInstance();

                reader.setHandler(obj);

                for (FieldInfo info : fields)
                    info.read(obj, reader);

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

    /**
     * @param cls Class.
     * @return Mode.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static Mode mode(Class<?> cls) {
        assert cls != null;

        if (cls == byte.class || cls == Byte.class)
            return Mode.BYTE;
        else if (cls == short.class || cls == Short.class)
            return Mode.SHORT;
        else if (cls == int.class || cls == Integer.class)
            return Mode.INT;
        else if (cls == long.class || cls == Long.class)
            return Mode.LONG;
        else if (cls == float.class || cls == Float.class)
            return Mode.FLOAT;
        else if (cls == double.class || cls == Double.class)
            return Mode.DOUBLE;
        else if (cls == char.class || cls == Character.class)
            return Mode.CHAR;
        else if (cls == boolean.class || cls == Boolean.class)
            return Mode.BOOLEAN;
        else if (cls == String.class)
            return Mode.STRING;
        else if (cls == UUID.class)
            return Mode.UUID;
        else if (cls == Date.class)
            return Mode.DATE;
        else if (cls == byte[].class)
            return Mode.BYTE_ARR;
        else if (cls == short[].class)
            return Mode.SHORT_ARR;
        else if (cls == int[].class)
            return Mode.INT_ARR;
        else if (cls == long[].class)
            return Mode.LONG_ARR;
        else if (cls == float[].class)
            return Mode.FLOAT_ARR;
        else if (cls == double[].class)
            return Mode.DOUBLE_ARR;
        else if (cls == char[].class)
            return Mode.CHAR_ARR;
        else if (cls == boolean[].class)
            return Mode.BOOLEAN_ARR;
        else if (cls == String[].class)
            return Mode.STRING_ARR;
        else if (cls == UUID[].class)
            return Mode.UUID_ARR;
        else if (cls == Date[].class)
            return Mode.DATE_ARR;
        else if (cls == Object[].class)
            return Mode.OBJ_ARR;
        else if (Collection.class.isAssignableFrom(cls))
            return Mode.COL;
        else if (Map.class.isAssignableFrom(cls))
            return Mode.MAP;
        else if (GridPortable.class.isAssignableFrom(cls))
            return Mode.PORTABLE;
        else
            return Mode.OBJECT;
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final int id;

        /** */
        private final Mode mode;

        /** */
        private final boolean prim;

        /**
         * @param field Field.
         * @param id Field ID.
         */
        private FieldInfo(Field field, int id) {
            assert field != null;

            this.field = field;
            this.id = id;

            Class<?> type = field.getType();

            mode = mode(type);
            prim = type.isPrimitive();
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

            switch (mode) {
                case BYTE:
                    if (prim)
                        writer.writeByteField((byte)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case SHORT:
                    if (prim)
                        writer.writeShortField((short)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case INT:
                    if (prim)
                        writer.writeIntField((int)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case LONG:
                    if (prim)
                        writer.writeLongField((long)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case FLOAT:
                    if (prim)
                        writer.writeFloatField((float)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case DOUBLE:
                    if (prim)
                        writer.writeDoubleField((double)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case CHAR:
                    if (prim)
                        writer.writeCharField((char)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case BOOLEAN:
                    if (prim)
                        writer.writeBooleanField((boolean)val);
                    else
                        writer.writeObjectField(val);

                    break;

                case STRING:
                    writer.writeStringField((String)val);

                    break;

                case UUID:
                    writer.writeUuidField((UUID)val);

                    break;

                case DATE:
                    writer.writeDateField((Date)val);

                    break;

                case BYTE_ARR:
                    writer.writeByteArrayField((byte[])val);

                    break;

                case SHORT_ARR:
                    writer.writeShortArrayField((short[])val);

                    break;

                case INT_ARR:
                    writer.writeIntArrayField((int[])val);

                    break;

                case LONG_ARR:
                    writer.writeLongArrayField((long[])val);

                    break;

                case FLOAT_ARR:
                    writer.writeFloatArrayField((float[])val);

                    break;

                case DOUBLE_ARR:
                    writer.writeDoubleArrayField((double[])val);

                    break;

                case CHAR_ARR:
                    writer.writeCharArrayField((char[])val);

                    break;

                case BOOLEAN_ARR:
                    writer.writeBooleanArrayField((boolean[])val);

                    break;

                case STRING_ARR:
                    writer.writeStringArrayField((String[])val);

                    break;

                case UUID_ARR:
                    writer.writeUuidArrayField((UUID[])val);

                    break;

                case DATE_ARR:
                    writer.writeDateArrayField((Date[])val);

                    break;

                case OBJ_ARR:
                    writer.writeObjectArrayField((Object[])val);

                    break;

                case COL:
                    writer.writeCollectionField((Collection<?>)val);

                    break;

                case MAP:
                    writer.writeMapField((Map<?, ?>)val);

                    break;

                case PORTABLE:
                case OBJECT:
                    writer.writeObjectField(val);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }
        }

        /**
         * @param obj Object.
         * @param reader Reader.
         * @throws GridPortableException In case of error.
         */
        public void read(Object obj, GridPortableReaderImpl reader) throws GridPortableException {
            Object val = null;

            switch (mode) {
                case BYTE:
                    val = prim ? reader.readByte(id) : reader.readObject(id);

                    break;

                case SHORT:
                    val = prim ? reader.readShort(id) : reader.readObject(id);

                    break;

                case INT:
                    val = prim ? reader.readInt(id) : reader.readObject(id);

                    break;

                case LONG:
                    val = prim ? reader.readLong(id) : reader.readObject(id);

                    break;

                case FLOAT:
                    val = prim ? reader.readFloat(id) : reader.readObject(id);

                    break;

                case DOUBLE:
                    val = prim ? reader.readDouble(id) : reader.readObject(id);

                    break;

                case CHAR:
                    val = prim ? reader.readChar(id) : reader.readObject(id);

                    break;

                case BOOLEAN:
                    val = prim ? reader.readBoolean(id) : reader.readObject(id);

                    break;

                case STRING:
                    val = reader.readString(id);

                    break;

                case UUID:
                    val = reader.readUuid(id);

                    break;

                case DATE:
                    val = reader.readDate(id);

                    break;

                case BYTE_ARR:
                    val = reader.readByteArray(id);

                    break;

                case SHORT_ARR:
                    val = reader.readShortArray(id);

                    break;

                case INT_ARR:
                    val = reader.readIntArray(id);

                    break;

                case LONG_ARR:
                    val = reader.readLongArray(id);

                    break;

                case FLOAT_ARR:
                    val = reader.readFloatArray(id);

                    break;

                case DOUBLE_ARR:
                    val = reader.readDoubleArray(id);

                    break;

                case CHAR_ARR:
                    val = reader.readCharArray(id);

                    break;

                case BOOLEAN_ARR:
                    val = reader.readBooleanArray(id);

                    break;

                case STRING_ARR:
                    val = reader.readStringArray(id);

                    break;

                case UUID_ARR:
                    val = reader.readUuidArray(id);

                    break;

                case DATE_ARR:
                    val = reader.readDateArray(id);

                    break;

                case OBJ_ARR:
                    val = reader.readObjectArray(id);

                    break;

                case COL:
                    val = reader.readCollection(id, null);

                    break;

                case MAP:
                    val = reader.readMap(id, null);

                    break;

                case PORTABLE:
                case OBJECT:
                    val = reader.readObject(id);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }

            try {
                field.set(obj, val);
            }
            catch (IllegalAccessException e) {
                throw new GridPortableException("Failed to set value for field: " + field, e);
            }
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
        DATE,

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
        DATE_ARR,

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
