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

package org.apache.ignite.internal.binary;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReflectiveSerializer;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Binary class descriptor.
 */
public class BinaryClassDescriptor {
    /** */
    @GridToStringExclude
    private final BinaryContext ctx;

    /** */
    private final Class<?> cls;

    /** Configured serializer. */
    private final BinarySerializer serializer;

    /** Serializer that is passed during BinaryClassDescriptor construction. Can differ from {@link #serializer}. */
    private final BinarySerializer initialSerializer;

    /** ID mapper. */
    private final BinaryInternalMapper mapper;

    /** */
    private final BinaryWriteMode mode;

    /** */
    private final boolean userType;

    /** */
    private final int typeId;

    /** */
    private final String typeName;

    /** Affinity key field name. */
    private final String affKeyFieldName;

    /** */
    private final Constructor<?> ctor;

    /** */
    private final BinaryFieldAccessor[] fields;

    /** Write replacer. */
    private final BinaryWriteReplacer writeReplacer;

    /** */
    private final Method readResolveMtd;

    /** */
    private final Map<String, Integer> stableFieldsMeta;

    /** Object schemas. Initialized only for serializable classes and contains only 1 entry. */
    private final BinarySchema stableSchema;

    /** Schema registry. */
    private final BinarySchemaRegistry schemaReg;

    /** */
    private final boolean registered;

    /** */
    private final boolean useOptMarshaller;

    /** */
    private final boolean excluded;

    /** */
    private final boolean overridesHashCode;

    /** */
    private final Class<?>[] intfs;

    /** Whether stable schema was published. */
    private volatile boolean stableSchemaPublished;

    /**
     * @param ctx Context.
     * @param cls Class.
     * @param userType User type flag.
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param affKeyFieldName Affinity key field name.
     * @param mapper Mapper.
     * @param serializer Serializer.
     * @param metaDataEnabled Metadata enabled flag.
     * @param registered Whether typeId has been successfully registered by MarshallerContext or not.
     * @throws BinaryObjectException In case of error.
     */
    BinaryClassDescriptor(
        BinaryContext ctx,
        Class<?> cls,
        boolean userType,
        int typeId,
        String typeName,
        @Nullable String affKeyFieldName,
        @Nullable BinaryInternalMapper mapper,
        @Nullable BinarySerializer serializer,
        boolean metaDataEnabled,
        boolean registered
    ) throws BinaryObjectException {
        assert ctx != null;
        assert cls != null;
        assert mapper != null;

        initialSerializer = serializer;

        // If serializer is not defined at this point, then we have to use OptimizedMarshaller.
        useOptMarshaller = serializer == null || GridQueryProcessor.isGeometryClass(cls);

        // Reset reflective serializer so that we rely on existing reflection-based serialization.
        if (serializer instanceof BinaryReflectiveSerializer)
            serializer = null;

        this.ctx = ctx;
        this.cls = cls;
        this.typeId = typeId;
        this.userType = userType;
        this.typeName = typeName;
        this.affKeyFieldName = affKeyFieldName;
        this.serializer = serializer;
        this.mapper = mapper;
        this.registered = registered;

        overridesHashCode = IgniteUtils.overridesEqualsAndHashCode(cls);

        schemaReg = ctx.schemaRegistry(typeId);

        excluded = MarshallerExclusions.isExcluded(cls);

        if (excluded)
            mode = BinaryWriteMode.EXCLUSION;
        else if (useOptMarshaller)
            mode = BinaryWriteMode.OPTIMIZED; // Will not be used anywhere.
        else {
            if (cls == BinaryEnumObjectImpl.class)
                mode = BinaryWriteMode.BINARY_ENUM;
            else
                mode = serializer != null ? BinaryWriteMode.BINARY : BinaryUtils.mode(cls);
        }

        if (useOptMarshaller && userType && !U.isIgnite(cls) && !U.isJdk(cls) &&
            !GridQueryProcessor.isGeometryClass(cls)) {
            U.warn(ctx.log(), "Class \"" + cls.getName() + "\" cannot be serialized using " +
                BinaryMarshaller.class.getSimpleName() + " because it either implements Externalizable interface " +
                "or have writeObject/readObject methods. " + OptimizedMarshaller.class.getSimpleName() + " will be " +
                "used instead and class instances will be deserialized on the server. Please ensure that all nodes " +
                "have this class in classpath. To enable binary serialization either implement " +
                Binarylizable.class.getSimpleName() + " interface or set explicit serializer using " +
                "BinaryTypeConfiguration.setSerializer() method.");
        }

        switch (mode) {
            case P_BYTE:
            case P_BOOLEAN:
            case P_SHORT:
            case P_CHAR:
            case P_INT:
            case P_LONG:
            case P_FLOAT:
            case P_DOUBLE:
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
            case OBJECT_ARR:
            case COL:
            case MAP:
            case BINARY_OBJ:
            case ENUM:
            case BINARY_ENUM:
            case ENUM_ARR:
            case CLASS:
            case OPTIMIZED:
            case EXCLUSION:
                ctor = null;
                fields = null;
                stableFieldsMeta = null;
                stableSchema = null;
                intfs = null;

                break;

            case PROXY:
                ctor = null;
                fields = null;
                stableFieldsMeta = null;
                stableSchema = null;
                intfs = cls.getInterfaces();

                break;

            case BINARY:
                ctor = constructor(cls);
                fields = null;
                stableFieldsMeta = null;
                stableSchema = null;
                intfs = null;

                break;

            case OBJECT:
                // Must not use constructor to honor transient fields semantics.
                ctor = null;

                Map<Object, BinaryFieldAccessor> fields0;

                if (BinaryUtils.FIELDS_SORTED_ORDER) {
                    fields0 = new TreeMap<>();

                    stableFieldsMeta = metaDataEnabled ? new TreeMap<String, Integer>() : null;
                }
                else {
                    fields0 = new LinkedHashMap<>();

                    stableFieldsMeta = metaDataEnabled ? new LinkedHashMap<String, Integer>() : null;
                }

                Set<String> duplicates = duplicateFields(cls);

                Collection<String> names = new HashSet<>();
                Collection<Integer> ids = new HashSet<>();

                for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                    for (Field f : c.getDeclaredFields()) {
                        if (serializeField(f)) {
                            f.setAccessible(true);

                            String name = f.getName();

                            if (duplicates.contains(name))
                                name = BinaryUtils.qualifiedFieldName(c, name);

                            boolean added = names.add(name);

                            assert added : name;

                            int fieldId = this.mapper.fieldId(typeId, name);

                            if (!ids.add(fieldId))
                                throw new BinaryObjectException("Duplicate field ID: " + name);

                            BinaryFieldAccessor fieldInfo = BinaryFieldAccessor.create(f, fieldId);

                            fields0.put(name, fieldInfo);

                            if (metaDataEnabled)
                                stableFieldsMeta.put(name, fieldInfo.mode().typeId());
                        }
                    }
                }

                fields = fields0.values().toArray(new BinaryFieldAccessor[fields0.size()]);

                BinarySchema.Builder schemaBuilder = BinarySchema.Builder.newBuilder();

                for (BinaryFieldAccessor field : fields)
                    schemaBuilder.addField(field.id);

                stableSchema = schemaBuilder.build();

                intfs = null;

                break;

            default:
                // Should never happen.
                throw new BinaryObjectException("Invalid mode: " + mode);
        }

        BinaryWriteReplacer writeReplacer0 = BinaryUtils.writeReplacer(cls);

        Method writeReplaceMthd;

        if (mode == BinaryWriteMode.BINARY || mode == BinaryWriteMode.OBJECT) {
            readResolveMtd = U.findNonPublicMethod(cls, "readResolve");

            writeReplaceMthd = U.findNonPublicMethod(cls, "writeReplace");
        }
        else {
            readResolveMtd = null;
            writeReplaceMthd = null;
        }

        if (writeReplaceMthd != null && writeReplacer0 == null)
            writeReplacer0 = new BinaryMethodWriteReplacer(writeReplaceMthd);

        writeReplacer = writeReplacer0;
    }

    /**
     * Find all fields with duplicate names in the class.
     *
     * @param cls Class.
     * @return Fields with duplicate names.
     */
    private static Set<String> duplicateFields(Class cls) {
        Set<String> all = new HashSet<>();
        Set<String> duplicates = new HashSet<>();

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (serializeField(f)) {
                    String name = f.getName();

                    if (!all.add(name))
                        duplicates.add(name);
                }
            }
        }

        return duplicates;
    }

    /**
     * Whether the field must be serialized.
     *
     * @param f Field.
     * @return {@code True} if must be serialized.
     */
    private static boolean serializeField(Field f) {
        int mod = f.getModifiers();

        return !Modifier.isStatic(mod) && !Modifier.isTransient(mod);
    }

    /**
     * @return {@code True} if enum.
     */
    boolean isEnum() {
        return mode == BinaryWriteMode.ENUM;
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
     * @return Type name.
     */
    String typeName() {
        return typeName;
    }

    /**
     * @return Type mapper.
     */
    BinaryInternalMapper mapper() {
        return mapper;
    }

    /**
     * @return Serializer.
     */
    BinarySerializer serializer() {
        return serializer;
    }

    /**
     * @return Initial serializer that is passed during BinaryClassDescriptor construction.
     * Can differ from {@link #serializer}.
     */
    BinarySerializer initialSerializer() {
        return initialSerializer;
    }

    /**
     * @return Affinity field key name.
     */
    String affFieldKeyName() {
        return affKeyFieldName;
    }

    /**
     * @return User type flag.
     */
    boolean userType() {
        return userType;
    }

    /**
     * @return Fields meta data.
     */
    Map<String, Integer> fieldsMeta() {
        return stableFieldsMeta;
    }

    /**
     * @return Schema.
     */
    BinarySchema schema() {
        return stableSchema;
    }

    /**
     * @return Whether typeId has been successfully registered by MarshallerContext or not.
     */
    public boolean registered() {
        return registered;
    }

    /**
     * @return {@code true} if {@link OptimizedMarshaller} must be used instead of {@link BinaryMarshaller}
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
     * @return {@code True} if write-replace should be performed for class.
     */
    public boolean isWriteReplace() {
        return writeReplacer != null;
    }

    /**
     * Perform write replace.
     *
     * @param obj Original object.
     * @return Replaced object.
     */
    public Object writeReplace(Object obj) {
        assert isWriteReplace();

        return writeReplacer.replace(obj);
    }

    /**
     * @return binaryReadResolve() method
     */
    @SuppressWarnings("UnusedDeclaration")
    @Nullable Method getReadResolveMethod() {
        return readResolveMtd;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws BinaryObjectException In case of error.
     */
    void write(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
        assert obj != null;
        assert writer != null;
        assert mode != BinaryWriteMode.OPTIMIZED : "OptimizedMarshaller should not be used here: " + cls.getName();

        writer.typeId(typeId);

        switch (mode) {
            case P_BYTE:
            case BYTE:
                writer.writeByteFieldPrimitive((byte) obj);

                break;

            case P_SHORT:
            case SHORT:
                writer.writeShortFieldPrimitive((short)obj);

                break;

            case P_INT:
            case INT:
                writer.writeIntFieldPrimitive((int) obj);

                break;

            case P_LONG:
            case LONG:
                writer.writeLongFieldPrimitive((long) obj);

                break;

            case P_FLOAT:
            case FLOAT:
                writer.writeFloatFieldPrimitive((float) obj);

                break;

            case P_DOUBLE:
            case DOUBLE:
                writer.writeDoubleFieldPrimitive((double) obj);

                break;

            case P_CHAR:
            case CHAR:
                writer.writeCharFieldPrimitive((char) obj);

                break;

            case P_BOOLEAN:
            case BOOLEAN:
                writer.writeBooleanFieldPrimitive((boolean) obj);

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

            case OBJECT_ARR:
                writer.doWriteObjectArray((Object[])obj);

                break;

            case COL:
                writer.doWriteCollection((Collection<?>)obj);

                break;

            case MAP:
                writer.doWriteMap((Map<?, ?>)obj);

                break;

            case ENUM:
                writer.doWriteEnum((Enum<?>)obj);

                break;

            case BINARY_ENUM:
                writer.doWriteBinaryEnum((BinaryEnumObjectImpl)obj);

                break;

            case ENUM_ARR:
                writer.doWriteEnumArray((Object[])obj);

                break;

            case CLASS:
                writer.doWriteClass((Class)obj);

                break;

            case PROXY:
                writer.doWriteProxy((Proxy)obj, intfs);

                break;

            case BINARY_OBJ:
                writer.doWriteBinaryObject((BinaryObjectImpl)obj);

                break;

            case BINARY:
                if (preWrite(writer, obj)) {
                    try {
                        if (serializer != null)
                            serializer.writeBinary(obj, writer);
                        else
                            ((Binarylizable)obj).writeBinary(writer);

                        postWrite(writer, obj);

                        // Check whether we need to update metadata.
                        if (obj.getClass() != BinaryMetadata.class) {
                            int schemaId = writer.schemaId();

                            if (schemaReg.schema(schemaId) == null) {
                                // This is new schema, let's update metadata.
                                BinaryMetadataCollector collector =
                                    new BinaryMetadataCollector(typeId, typeName, mapper);

                                if (serializer != null)
                                    serializer.writeBinary(obj, collector);
                                else
                                    ((Binarylizable)obj).writeBinary(collector);

                                BinarySchema newSchema = collector.schema();

                                BinaryMetadata meta = new BinaryMetadata(typeId, typeName, collector.meta(),
                                    affKeyFieldName, Collections.singleton(newSchema), false);

                                ctx.updateMetadata(typeId, meta);

                                schemaReg.addSchema(newSchema.schemaId(), newSchema);
                            }
                        }

                        postWriteHashCode(writer, obj);
                    }
                    finally {
                        writer.popSchema();
                    }
                }

                break;

            case OBJECT:
                if (userType && !stableSchemaPublished) {
                    // Update meta before write object with new schema
                    BinaryMetadata meta = new BinaryMetadata(typeId, typeName, stableFieldsMeta,
                        affKeyFieldName, Collections.singleton(stableSchema), false);

                    ctx.updateMetadata(typeId, meta);

                    schemaReg.addSchema(stableSchema.schemaId(), stableSchema);

                    stableSchemaPublished = true;
                }

                if (preWrite(writer, obj)) {
                    try {
                        for (BinaryFieldAccessor info : fields)
                            info.write(obj, writer);

                        writer.schemaId(stableSchema.schemaId());

                        postWrite(writer, obj);
                        postWriteHashCode(writer, obj);
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
     * @throws BinaryObjectException If failed.
     */
    Object read(BinaryReaderExImpl reader) throws BinaryObjectException {
        assert reader != null;
        assert mode != BinaryWriteMode.OPTIMIZED : "OptimizedMarshaller should not be used here: " + cls.getName();

        Object res;

        switch (mode) {
            case BINARY:
                res = newInstance();

                reader.setHandle(res);

                if (serializer != null)
                    serializer.readBinary(res, reader);
                else
                    ((Binarylizable)res).readBinary(reader);

                break;

            case OBJECT:
                res = newInstance();

                reader.setHandle(res);

                for (BinaryFieldAccessor info : fields)
                    info.read(res, reader);

                break;

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }

        if (readResolveMtd != null) {
            try {
                res = readResolveMtd.invoke(res);

                reader.setHandle(res);
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
     * Pre-write phase.
     *
     * @param writer Writer.
     * @param obj Object.
     * @return Whether further write is needed.
     */
    private boolean preWrite(BinaryWriterExImpl writer, Object obj) {
        if (writer.tryWriteAsHandle(obj))
            return false;

        writer.preWrite(registered ? null : cls.getName());

        return true;
    }

    /**
     * Post-write phase.
     *
     * @param writer Writer.
     * @param obj Object.
     */
    private void postWrite(BinaryWriterExImpl writer, Object obj) {
        if (obj instanceof CacheObjectImpl)
            writer.postWrite(userType, registered, 0, false);
        else if (obj instanceof BinaryObjectEx) {
            boolean flagSet = ((BinaryObjectEx)obj).isFlagSet(BinaryUtils.FLAG_EMPTY_HASH_CODE);

            writer.postWrite(userType, registered, obj.hashCode(), !flagSet);
        }
        else
            writer.postWrite(userType, registered, obj.hashCode(), overridesHashCode);
    }

    /**
     * Post-write routine for hash code.
     *
     * @param writer Writer.
     * @param obj Object.
     */
    private void postWriteHashCode(BinaryWriterExImpl writer, Object obj) {
        // No need to call "postWriteHashCode" here because we do not care about hash code.
        if (!(obj instanceof CacheObjectImpl))
            writer.postWriteHashCode(registered ? null : cls.getName());
    }

    /**
     * @return Instance.
     * @throws BinaryObjectException In case of error.
     */
    private Object newInstance() throws BinaryObjectException {
        try {
            return ctor != null ? ctor.newInstance() : GridUnsafe.allocateInstance(cls);
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new BinaryObjectException("Failed to instantiate instance: " + cls, e);
        }
    }

    /**
     * @param cls Class.
     * @return Constructor.
     * @throws BinaryObjectException If constructor doesn't exist.
     */
    @SuppressWarnings("ConstantConditions")
    @Nullable private static Constructor<?> constructor(Class<?> cls) throws BinaryObjectException {
        assert cls != null;

        try {
            Constructor<?> ctor = U.forceEmptyConstructor(cls);

            if (ctor == null)
                throw new BinaryObjectException("Failed to find empty constructor for class: " + cls.getName());

            ctor.setAccessible(true);

            return ctor;
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to get constructor for class: " + cls.getName(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryClassDescriptor.class, this);
    }
}
