/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jdk8.backport.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.reflect.Modifier.*;
import static org.gridgain.grid.kernal.portable.GridPortableMarshaller.*;

/**
 * Portable class descriptor.
 */
class GridPortableClassDescriptor {
    /** */
    private static final ConcurrentMap<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    /** */
    private static final int TOTAL_LEN_POS = 9;

    /** */
    private static final int COL_TYPE_ID = 100;

    /** */
    private static final int MAP_TYPE_ID = 200;

    /** */
    private final Mode mode;

    /** */
    private final int typeId;

    /** */
    private List<FieldInfo> fields;

    /** */
    private byte[] hdr;

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
     * @param mode Mode.
     * @param typeId Type ID.
     */
    private GridPortableClassDescriptor(Mode mode, int typeId) {
        assert mode != null;

        this.mode = mode;
        this.typeId = typeId;
    }

    /**
     * @param cls Class.
     */
    private GridPortableClassDescriptor(Class<?> cls) throws GridPortableException {
        assert cls != null;

        if (Collection.class.isAssignableFrom(cls)) {
            mode = Mode.COL;

            typeId = COL_TYPE_ID;
        }
        else if (Map.class.isAssignableFrom(cls)) {
            mode = Mode.MAP;

            typeId = MAP_TYPE_ID;
        }
        else {
            if (GridPortableEx.class.isAssignableFrom(cls))
                mode = Mode.PORTABLE_EX;
            else {
                mode = Mode.PORTABLE;

                fields = new ArrayList<>();

                GridPortableHeaderWriter hdrWriter = new GridPortableHeaderWriter();

                for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                    for (Field f : c.getDeclaredFields()) {
                        int mod = f.getModifiers();

                        if (!isStatic(mod) && !isTransient(mod)) {
                            f.setAccessible(true);

                            int offPos = hdrWriter.addField(f.getName());

                            fields.add(new FieldInfo(f, offPos));
                        }
                    }
                }

                hdr = hdrWriter.header();
            }

            typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config
        }
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

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    void write(Object obj, GridPortableWriterImpl writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        writer.doWriteByte(OBJ);
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

            case PORTABLE_EX:
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                // Header handles are not supported for GridPortableEx.
                writer.doWriteInt(-1);

                writePortableEx((GridPortableEx)obj, writer);

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            case PORTABLE:
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                writer.doWriteInt(-1); // TODO: Header handles.
                writer.write(hdr);

                for (FieldInfo info : fields) {
                    writer.writeCurrentSize(info.offPos);

                    try {
                        writer.doWriteObject(info.field.get(obj));
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

            case PORTABLE_EX:
                return null; // TODO

            case PORTABLE:
                return null; // TODO

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    private void writePortableEx(GridPortableEx obj, GridPortableWriterImpl writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        GridPortableWriterImpl writer0 = new GridPortableWriterImpl(writer);

        obj.writePortable(writer0);

        writer0.flush();
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final int offPos;

        /**
         * @param field Field.
         * @param offPos Offset position.
         */
        private FieldInfo(Field field, int offPos) {
            this.field = field;
            this.offPos = offPos;
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
        PORTABLE_EX,

        /** */
        PORTABLE
    }
}
