/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.portable;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryTypeIdMapper;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinarySerializer;

import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;

/**
 * Portable class descriptor.
 */
public class PortableClassDescriptor {
    /** */
    private final PortableContext ctx;

    /** */
    private final Class<?> cls;

    /** */
    private final BinarySerializer serializer;

    /** ID mapper. */
    private final BinaryTypeIdMapper idMapper;

    /** */
    private final Mode mode;

    /** */
    private final boolean userType;

    /** */
    private final int typeId;

    /** */
    private final String typeName;

    /** */
    private final Constructor<?> ctor;

    /** */
    private final Collection<FieldInfo> fields;

    /** */
    private final Method writeReplaceMtd;

    /** */
    private final Method readResolveMtd;

    /** */
    private final Map<String, String> fieldsMeta;

    /** */
    private final boolean keepDeserialized;

    /** */
    private final boolean registered;

    /** */
    private final boolean useOptMarshaller;

    /** */
    private final boolean excluded;

    /**
     * @param ctx Context.
     * @param cls Class.
     * @param userType User type flag.
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param idMapper ID mapper.
     * @param serializer Serializer.
     * @param metaDataEnabled Metadata enabled flag.
     * @param keepDeserialized Keep deserialized flag.
     * @param registered Whether typeId has been successfully registered by MarshallerContext or not.
     * @param predefined Whether the class is predefined or not.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    PortableClassDescriptor(
        PortableContext ctx,
        Class<?> cls,
        boolean userType,
        int typeId,
        String typeName,
        @Nullable BinaryTypeIdMapper idMapper,
        @Nullable BinarySerializer serializer,
        boolean metaDataEnabled,
        boolean keepDeserialized,
        boolean registered,
        boolean predefined
    ) throws BinaryObjectException {
        assert ctx != null;
        assert cls != null;

        this.ctx = ctx;
        this.cls = cls;
        this.userType = userType;
        this.typeId = typeId;
        this.typeName = typeName;
        this.serializer = serializer;
        this.idMapper = idMapper;
        this.keepDeserialized = keepDeserialized;
        this.registered = registered;

        excluded = MarshallerExclusions.isExcluded(cls);

        useOptMarshaller = !predefined && initUseOptimizedMarshallerFlag();

        if (excluded)
            mode = Mode.EXCLUSION;
        else
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
            case DECIMAL:
            case STRING:
            case UUID:
            case DATE:
            case TIMESTAMP:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case DECIMAL_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case TIMESTAMP_ARR:
            case OBJ_ARR:
            case COL:
            case MAP:
            case MAP_ENTRY:
            case PORTABLE_OBJ:
            case ENUM:
            case ENUM_ARR:
            case CLASS:
            case EXCLUSION:
                ctor = null;
                fields = null;
                fieldsMeta = null;

                break;

            case PORTABLE:
            case EXTERNALIZABLE:
                ctor = constructor(cls);
                fields = null;
                fieldsMeta = null;

                break;

            case OBJECT:
                assert idMapper != null;

                ctor = constructor(cls);
                fields = new ArrayList<>();
                fieldsMeta = metaDataEnabled ? new HashMap<String, String>() : null;

                Collection<String> names = new HashSet<>();
                Collection<Integer> ids = new HashSet<>();

                for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                    for (Field f : c.getDeclaredFields()) {
                        int mod = f.getModifiers();

                        if (!isStatic(mod) && !isTransient(mod)) {
                            f.setAccessible(true);

                            String name = f.getName();

                            if (!names.add(name))
                                throw new BinaryObjectException("Duplicate field name: " + name);

                            int fieldId = idMapper.fieldId(typeId, name);

                            if (!ids.add(fieldId))
                                throw new BinaryObjectException("Duplicate field ID: " + name);

                            FieldInfo fieldInfo = new FieldInfo(f, fieldId);

                            fields.add(fieldInfo);

                            if (metaDataEnabled)
                                fieldsMeta.put(name, fieldInfo.fieldMode().typeName());
                        }
                    }
                }

                break;

            default:
                // Should never happen.
                throw new BinaryObjectException("Invalid mode: " + mode);
        }

        if (mode == Mode.PORTABLE || mode == Mode.EXTERNALIZABLE || mode == Mode.OBJECT) {
            readResolveMtd = U.findNonPublicMethod(cls, "readResolve");
            writeReplaceMtd = U.findNonPublicMethod(cls, "writeReplace");
        }
        else {
            readResolveMtd = null;
            writeReplaceMtd = null;
        }
    }

    /**
     * @return Described class.
     */
    Class<?> describedClass() {
        return cls;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return User type flag.
     */
    public boolean userType() {
        return userType;
    }

    /**
     * @return Fields meta data.
     */
    Map<String, String> fieldsMeta() {
        return fieldsMeta;
    }

    /**
     * @return Keep deserialized flag.
     */
    boolean keepDeserialized() {
        return keepDeserialized;
    }

    /**
     * @return Whether typeId has been successfully registered by MarshallerContext or not.
     */
    public boolean registered() {
        return registered;
    }

    /**
     * @return {@code true} if {@link OptimizedMarshaller} must be used instead of {@link PortableMarshaller}
     * for object serialization and deserialization.
     */
    public boolean useOptimizedMarshaller() {
        return useOptMarshaller;
    }

    /**
     * Checks whether the class values are explicitly excluded from marshalling.
     *
     * @return {@code true} if excluded, {@code false} otherwise.
     */
    public boolean excluded() {
        return excluded;
    }

    /**
     * Get ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryTypeIdMapper idMapper() {
        return idMapper;
    }

    /**
     * @return portableWriteReplace() method
     */
    @Nullable Method getWriteReplaceMethod() {
        return writeReplaceMtd;
    }

    /**
     * @return portableReadResolve() method
     */
    @SuppressWarnings("UnusedDeclaration")
    @Nullable Method getReadResolveMethod() {
        return readResolveMtd;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void write(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
        assert obj != null;
        assert writer != null;

        switch (mode) {
            case BYTE:
                writer.doWriteByte(GridPortableMarshaller.BYTE);
                writer.doWriteByte((byte)obj);

                break;

            case SHORT:
                writer.doWriteByte(GridPortableMarshaller.SHORT);
                writer.doWriteShort((short)obj);

                break;

            case INT:
                writer.doWriteByte(GridPortableMarshaller.INT);
                writer.doWriteInt((int)obj);

                break;

            case LONG:
                writer.doWriteByte(GridPortableMarshaller.LONG);
                writer.doWriteLong((long)obj);

                break;

            case FLOAT:
                writer.doWriteByte(GridPortableMarshaller.FLOAT);
                writer.doWriteFloat((float)obj);

                break;

            case DOUBLE:
                writer.doWriteByte(GridPortableMarshaller.DOUBLE);
                writer.doWriteDouble((double)obj);

                break;

            case CHAR:
                writer.doWriteByte(GridPortableMarshaller.CHAR);
                writer.doWriteChar((char)obj);

                break;

            case BOOLEAN:
                writer.doWriteByte(GridPortableMarshaller.BOOLEAN);
                writer.doWriteBoolean((boolean)obj);

                break;

            case DECIMAL:
                writer.doWriteDecimal((BigDecimal)obj);

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

            case TIMESTAMP:
                writer.doWriteTimestamp((Timestamp)obj);

                break;

            case BYTE_ARR:
                writer.doWriteByteArray((byte[])obj);

                break;

            case SHORT_ARR:
                writer.doWriteShortArray((short[]) obj);

                break;

            case INT_ARR:
                writer.doWriteIntArray((int[]) obj);

                break;

            case LONG_ARR:
                writer.doWriteLongArray((long[]) obj);

                break;

            case FLOAT_ARR:
                writer.doWriteFloatArray((float[]) obj);

                break;

            case DOUBLE_ARR:
                writer.doWriteDoubleArray((double[]) obj);

                break;

            case CHAR_ARR:
                writer.doWriteCharArray((char[]) obj);

                break;

            case BOOLEAN_ARR:
                writer.doWriteBooleanArray((boolean[]) obj);

                break;

            case DECIMAL_ARR:
                writer.doWriteDecimalArray((BigDecimal[]) obj);

                break;

            case STRING_ARR:
                writer.doWriteStringArray((String[]) obj);

                break;

            case UUID_ARR:
                writer.doWriteUuidArray((UUID[]) obj);

                break;

            case DATE_ARR:
                writer.doWriteDateArray((Date[]) obj);

                break;

            case TIMESTAMP_ARR:
                writer.doWriteTimestampArray((Timestamp[]) obj);

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

            case MAP_ENTRY:
                writer.doWriteMapEntry((Map.Entry<?, ?>)obj);

                break;

            case ENUM:
                writer.doWriteEnum((Enum<?>)obj);

                break;

            case ENUM_ARR:
                writer.doWriteEnumArray((Object[])obj);

                break;

            case CLASS:
                writer.doWriteClass((Class)obj);

                break;

            case PORTABLE_OBJ:
                writer.doWritePortableObject((BinaryObjectImpl)obj);

                break;

            case PORTABLE:
                if (writeHeader(obj, writer)) {
                    try {
                        if (serializer != null)
                            serializer.writeBinary(obj, writer);
                        else
                            ((Binarylizable)obj).writeBinary(writer);

                        writer.postWrite(userType);
                    }
                    finally {
                        writer.popSchema();
                    }

                    if (obj.getClass() != BinaryMetaDataImpl.class
                        && ctx.isMetaDataChanged(typeId, writer.metaDataHashSum())) {
                        BinaryMetaDataCollector metaCollector = new BinaryMetaDataCollector(typeName);

                        if (serializer != null)
                            serializer.writeBinary(obj, metaCollector);
                        else
                            ((Binarylizable)obj).writeBinary(metaCollector);

                        ctx.updateMetaData(typeId, typeName, metaCollector.meta());
                    }
                }

                break;

            case EXTERNALIZABLE:
                if (writeHeader(obj, writer)) {
                    writer.rawWriter();

                    try {
                        ((Externalizable)obj).writeExternal(writer);

                        writer.postWrite(userType);
                    }
                    catch (IOException e) {
                        throw new BinaryObjectException("Failed to write Externalizable object: " + obj, e);
                    }
                    finally {
                        writer.popSchema();
                    }
                }

                break;

            case OBJECT:
                if (writeHeader(obj, writer)) {
                    try {
                        for (FieldInfo info : fields)
                            info.write(obj, writer);

                        writer.postWrite(userType);
                    }
                    finally {
                        writer.popSchema();
                    }
                }

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /**
     * @param reader Reader.
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    Object read(BinaryReaderExImpl reader) throws BinaryObjectException {
        assert reader != null;

        Object res;

        switch (mode) {
            case PORTABLE:
                res = newInstance();

                reader.setHandler(res);

                if (serializer != null)
                    serializer.readBinary(res, reader);
                else
                    ((Binarylizable)res).readBinary(reader);

                break;

            case EXTERNALIZABLE:
                res = newInstance();

                reader.setHandler(res);

                try {
                    ((Externalizable)res).readExternal(reader);
                }
                catch (IOException | ClassNotFoundException e) {
                    throw new BinaryObjectException("Failed to read Externalizable object: " +
                        res.getClass().getName(), e);
                }

                break;

            case OBJECT:
                res = newInstance();

                reader.setHandler(res);

                for (FieldInfo info : fields)
                    info.read(res, reader);

                break;

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }

        if (readResolveMtd != null) {
            try {
                res = readResolveMtd.invoke(res);

                reader.setHandler(res);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof BinaryObjectException)
                    throw (BinaryObjectException)e.getTargetException();

                throw new BinaryObjectException("Failed to execute readResolve() method on " + res, e);
            }
        }

        return res;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @return Whether further write is needed.
     */
    private boolean writeHeader(Object obj, BinaryWriterExImpl writer) {
        if (writer.tryWriteAsHandle(obj))
            return false;

        PortableUtils.writeHeader(
            writer,
            userType,
            registered ? typeId : GridPortableMarshaller.UNREGISTERED_TYPE_ID,
            obj instanceof CacheObjectImpl ? 0 : obj.hashCode(),
            registered ? null : cls.getName()
        );

        return true;
    }

    /**
     * @return Instance.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Object newInstance() throws BinaryObjectException {
        assert ctor != null;

        try {
            return ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new BinaryObjectException("Failed to instantiate instance: " + cls, e);
        }
    }

    /**
     * @param cls Class.
     * @return Constructor.
     * @throws org.apache.ignite.binary.BinaryObjectException If constructor doesn't exist.
     */
    @SuppressWarnings("ConstantConditions")
    @Nullable private static Constructor<?> constructor(Class<?> cls) throws BinaryObjectException {
        assert cls != null;

        try {
            Constructor<?> ctor = U.forceEmptyConstructor(cls);

            ctor.setAccessible(true);

            return ctor;
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to get constructor for class: " + cls.getName(), e);
        }
    }

    /**
     * Determines whether to use {@link OptimizedMarshaller} for serialization or
     * not.
     *
     * @return {@code true} if to use, {@code false} otherwise.
     */
    private boolean initUseOptimizedMarshallerFlag() {
       boolean use;

        try {
            Method writeObj = cls.getDeclaredMethod("writeObject", ObjectOutputStream.class);
            Method readObj = cls.getDeclaredMethod("readObject", ObjectInputStream.class);

            use = !Modifier.isStatic(writeObj.getModifiers()) && !Modifier.isStatic(readObj.getModifiers()) &&
                writeObj.getReturnType() == void.class && readObj.getReturnType() == void.class;
        }
        catch (NoSuchMethodException e) {
            use = false;
        }

        return use;
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
        else if (cls == BigDecimal.class)
            return Mode.DECIMAL;
        else if (cls == String.class)
            return Mode.STRING;
        else if (cls == UUID.class)
            return Mode.UUID;
        else if (cls == Date.class)
            return Mode.DATE;
        else if (cls == Timestamp.class)
            return Mode.TIMESTAMP;
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
        else if (cls == BigDecimal[].class)
            return Mode.DECIMAL_ARR;
        else if (cls == String[].class)
            return Mode.STRING_ARR;
        else if (cls == UUID[].class)
            return Mode.UUID_ARR;
        else if (cls == Date[].class)
            return Mode.DATE_ARR;
        else if (cls == Timestamp[].class)
            return Mode.TIMESTAMP_ARR;
        else if (cls.isArray())
            return cls.getComponentType().isEnum() ? Mode.ENUM_ARR : Mode.OBJ_ARR;
        else if (cls == BinaryObjectImpl.class)
            return Mode.PORTABLE_OBJ;
        else if (Binarylizable.class.isAssignableFrom(cls))
            return Mode.PORTABLE;
        else if (Externalizable.class.isAssignableFrom(cls))
            return Mode.EXTERNALIZABLE;
        else if (Map.Entry.class.isAssignableFrom(cls))
            return Mode.MAP_ENTRY;
        else if (Collection.class.isAssignableFrom(cls))
            return Mode.COL;
        else if (Map.class.isAssignableFrom(cls))
            return Mode.MAP;
        else if (cls == BinaryObjectImpl.class)
            return Mode.PORTABLE_OBJ;
        else if (cls.isEnum())
            return Mode.ENUM;
        else if (cls == Class.class)
            return Mode.CLASS;
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
        }

        /**
         * @return Field mode.
         */
        public Mode fieldMode() {
            return mode;
        }

        /**
         * @param obj Object.
         * @param writer Writer.
         * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
         */
        public void write(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            assert obj != null;
            assert writer != null;

            writer.writeFieldId(id);

            Object val;

            try {
                val = field.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new BinaryObjectException("Failed to get value for field: " + field, e);
            }

            switch (mode) {
                case BYTE:
                    writer.writeByteField((Byte)val);

                    break;

                case SHORT:
                    writer.writeShortField((Short)val);

                    break;

                case INT:
                    writer.writeIntField((Integer)val);

                    break;

                case LONG:
                    writer.writeLongField((Long)val);

                    break;

                case FLOAT:
                    writer.writeFloatField((Float)val);

                    break;

                case DOUBLE:
                    writer.writeDoubleField((Double)val);

                    break;

                case CHAR:
                    writer.writeCharField((Character)val);

                    break;

                case BOOLEAN:
                    writer.writeBooleanField((Boolean)val);

                    break;

                case DECIMAL:
                    writer.writeDecimalField((BigDecimal)val);

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

                case TIMESTAMP:
                    writer.writeTimestampField((Timestamp)val);

                    break;

                case BYTE_ARR:
                    writer.writeByteArrayField((byte[])val);

                    break;

                case SHORT_ARR:
                    writer.writeShortArrayField((short[]) val);

                    break;

                case INT_ARR:
                    writer.writeIntArrayField((int[]) val);

                    break;

                case LONG_ARR:
                    writer.writeLongArrayField((long[]) val);

                    break;

                case FLOAT_ARR:
                    writer.writeFloatArrayField((float[]) val);

                    break;

                case DOUBLE_ARR:
                    writer.writeDoubleArrayField((double[]) val);

                    break;

                case CHAR_ARR:
                    writer.writeCharArrayField((char[]) val);

                    break;

                case BOOLEAN_ARR:
                    writer.writeBooleanArrayField((boolean[]) val);

                    break;

                case DECIMAL_ARR:
                    writer.writeDecimalArrayField((BigDecimal[]) val);

                    break;

                case STRING_ARR:
                    writer.writeStringArrayField((String[]) val);

                    break;

                case UUID_ARR:
                    writer.writeUuidArrayField((UUID[]) val);

                    break;

                case DATE_ARR:
                    writer.writeDateArrayField((Date[]) val);

                    break;

                case TIMESTAMP_ARR:
                    writer.writeTimestampArrayField((Timestamp[]) val);

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

                case MAP_ENTRY:
                    writer.writeMapEntryField((Map.Entry<?, ?>)val);

                    break;

                case PORTABLE_OBJ:
                    writer.writePortableObjectField((BinaryObjectImpl)val);

                    break;

                case ENUM:
                    writer.writeEnumField((Enum<?>)val);

                    break;

                case ENUM_ARR:
                    writer.writeEnumArrayField((Object[])val);

                    break;

                case PORTABLE:
                case EXTERNALIZABLE:
                case OBJECT:
                    writer.writeObjectField(val);

                    break;

                case CLASS:
                    writer.writeClassField((Class)val);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }
        }

        /**
         * @param obj Object.
         * @param reader Reader.
         * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
         */
        public void read(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            Object val = null;

            switch (mode) {
                case BYTE:
                    val = reader.readByte(id);

                    break;

                case SHORT:
                    val = reader.readShort(id);

                    break;

                case INT:
                    val = reader.readInt(id);

                    break;

                case LONG:
                    val = reader.readLong(id);

                    break;

                case FLOAT:
                    val = reader.readFloat(id);

                    break;

                case DOUBLE:
                    val = reader.readDouble(id);

                    break;

                case CHAR:
                    val = reader.readChar(id);

                    break;

                case BOOLEAN:
                    val = reader.readBoolean(id);

                    break;

                case DECIMAL:
                    val = reader.readDecimal(id);

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

                case TIMESTAMP:
                    val = reader.readTimestamp(id);

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

                case DECIMAL_ARR:
                    val = reader.readDecimalArray(id);

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

                case TIMESTAMP_ARR:
                    val = reader.readTimestampArray(id);

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

                case MAP_ENTRY:
                    val = reader.readMapEntry(id);

                    break;

                case PORTABLE_OBJ:
                    val = reader.readPortableObject(id);

                    break;

                case ENUM:
                    val = reader.readEnum(id, field.getType());

                    break;

                case ENUM_ARR:
                    val = reader.readEnumArray(id, field.getType().getComponentType());

                    break;

                case PORTABLE:
                case EXTERNALIZABLE:
                case OBJECT:
                    val = reader.readObject(id);

                    break;

                case CLASS:
                    val = reader.readClass(id);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }

            try {
                if (val != null || !field.getType().isPrimitive())
                    field.set(obj, val);
            }
            catch (IllegalAccessException e) {
                throw new BinaryObjectException("Failed to set value for field: " + field, e);
            }
        }
    }

    /** */
    enum Mode {
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
        DECIMAL(GridPortableMarshaller.DECIMAL),

        /** */
        STRING(GridPortableMarshaller.STRING),

        /** */
        UUID(GridPortableMarshaller.UUID),

        /** */
        DATE(GridPortableMarshaller.DATE),

        /** */
        TIMESTAMP(GridPortableMarshaller.TIMESTAMP),

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
        DECIMAL_ARR(GridPortableMarshaller.DECIMAL_ARR),

        /** */
        STRING_ARR(GridPortableMarshaller.STRING_ARR),

        /** */
        UUID_ARR(GridPortableMarshaller.UUID_ARR),

        /** */
        DATE_ARR(GridPortableMarshaller.DATE_ARR),

        /** */
        TIMESTAMP_ARR(GridPortableMarshaller.TIMESTAMP_ARR),

        /** */
        OBJ_ARR(GridPortableMarshaller.OBJ_ARR),

        /** */
        COL(GridPortableMarshaller.COL),

        /** */
        MAP(GridPortableMarshaller.MAP),

        /** */
        MAP_ENTRY(GridPortableMarshaller.MAP_ENTRY),

        /** */
        PORTABLE_OBJ(GridPortableMarshaller.OBJ),

        /** */
        ENUM(GridPortableMarshaller.ENUM),

        /** */
        ENUM_ARR(GridPortableMarshaller.ENUM_ARR),

        /** */
        CLASS(GridPortableMarshaller.CLASS),

        /** */
        PORTABLE(GridPortableMarshaller.PORTABLE_OBJ),

        /** */
        EXTERNALIZABLE(GridPortableMarshaller.OBJ),

        /** */
        OBJECT(GridPortableMarshaller.OBJ),

        /** */
        EXCLUSION(GridPortableMarshaller.OBJ);

        /** */
        private final int typeId;

        /**
         * @param typeId Type ID.
         */
        Mode(int typeId) {
            this.typeId = typeId;
        }

        /**
         * @return Type name.
         */
        String typeName() {
            return PortableUtils.fieldTypeName(typeId);
        }
    }
}
