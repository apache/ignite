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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnum;
import org.h2.api.CustomDataTypesHandler;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueJavaObject;
import org.jsr166.ConcurrentHashMap8;

import java.sql.Types;
import java.util.concurrent.ConcurrentMap;

/**
 * Custom data types handler implementation
 */
public class GridH2CustomDataTypesHandler implements CustomDataTypesHandler {
    /** */
    private static ConcurrentMap<Integer, EnumDataType> dataTypesById = new ConcurrentHashMap8<>();
    /** */
    private static ConcurrentMap<String, EnumDataType> dataTypesByName = new ConcurrentHashMap8<>();
    /** */
    private static ConcurrentMap<String, EnumDataType> dataTypesByClassName = new ConcurrentHashMap8<>();

    /** */
    private final static int INVALID_TYPE_ID_RANGE_BEGIN = -1;
    /** */
    private final static int INVALID_TYPE_ID_RANGE_END = 100;
    /** */
    private final static int ENUM_ORDER = 100_000;

    /**
     * Register enum type.
     *
     * @param typeId Type id.
     * @param typeName Type name.
     */
    public void registerEnum(int typeId, String typeName) {
        String name = IgniteH2Indexing.escapeName(typeName, false).toUpperCase();

        if (dataTypesById.containsKey(typeId))
            return;

        if ((INVALID_TYPE_ID_RANGE_BEGIN <= typeId) && (typeId <= INVALID_TYPE_ID_RANGE_END))
            throw new IgniteException("Enums with type id in range [" + INVALID_TYPE_ID_RANGE_BEGIN +
                    ", " + INVALID_TYPE_ID_RANGE_END + "] are prohibited");

        EnumDataType dataType = new EnumDataType(typeId, name, typeName);
        dataTypesById.put(typeId, dataType);
        dataTypesByClassName.put(typeName, dataType);
        dataTypesByName.put(name, dataType);
    }

    /** {@inheritDoc} */
    @Override public DataType getDataTypeByName(String name) {
        DataType result = dataTypesByName.get(name);
        if (result == null)
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "name: " + name);
        return result;
    }

    /** {@inheritDoc} */
    @Override public DataType getDataTypeById(int type) {
        DataType result = dataTypesById.get(type);
        if (result == null)
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type: " + type);
        return result;
    }

    /** {@inheritDoc} */
    @Override public int getDataTypeOrder(int type) {
        if (dataTypesById.containsKey(type))
            return ENUM_ORDER;
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getDataTypeClassName(int type) {
        EnumDataType dataType = dataTypesById.get(type);
        if (dataType == null)
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type: " + type);
        return dataType.className;
    }

    /** {@inheritDoc} */
    @Override public int getTypeIdFromClass(Class<?> cls) {
        DataType dataType = dataTypesByClassName.get(cls.getName());
        if (dataType == null)
            return Value.JAVA_OBJECT;
        return dataType.type;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int type, Object data, DataHandler dataHandler) {
        Integer typeId = type;

        if (type == Value.UNKNOWN) {
            DataType dataType = dataTypesByClassName.get(data.getClass().getName());
            if (dataType != null)
                typeId = dataType.type;
        }

        if (typeId == Value.UNKNOWN)
            return ValueJavaObject.getNoCopy(data, null, dataHandler);

        GridH2QueryContext qryCtx = GridH2QueryContext.get();
        assert qryCtx.get() != null;

        GridKernalContext ctx = qryCtx.get().kernalContext();
        try {
            if (data instanceof Integer)
                return fromOrdinal(ctx, typeId, (Integer) data);

            if (data instanceof String)
                return fromName(ctx, typeId, (String) data);

            if (data instanceof Enum)
                return fromEnum(ctx, typeId, (Enum)data);

            if (data instanceof BinaryEnumObjectImpl)
                return fromBinaryEnum(ctx, typeId, (BinaryEnumObjectImpl)data);
        } catch (IgniteCheckedException ex) {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, ex);
        }
        throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type: " + type);
    }

    /** {@inheritDoc} */
    @Override public Object getObject(Value value, Class<?> cls) {
        return value.getObject();
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAdd(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getAddProofType(int type) {
        return type;
    }

    /** {@inheritDoc} */
    @Override public Value convert(Value source, int targetType) {
        return convert(GridH2QueryContext.get().kernalContext(), source, targetType);
    }

    /**
     * Converts {@link org.h2.value.Value} to specified type.
     *
     * @param ctx Kernal context.
     * @param source Source value.
     * @param targetType Target value type.
     * @return Result.
     */
    private Value convert(GridKernalContext ctx, Value source, int targetType) {
        if (source.getType() == targetType)
            return source;

        if (source instanceof GridH2ValueEnum)
            return source.convertTo(targetType);

        if (dataTypesById.get(targetType) != null) {
            try {
                switch (source.getType()) {
                    case Value.INT:
                        return fromOrdinal(ctx, targetType, source.getInt());
                    case Value.STRING:
                        return fromName(ctx, targetType, source.getString());
                    case Value.JAVA_OBJECT:
                        if (source instanceof GridH2ValueCacheObject) {
                            CacheObject cacheObject = (CacheObject)source.getObject();
                            if (cacheObject instanceof BinaryEnumObjectImpl)
                                return fromBinaryEnum(ctx, targetType, (BinaryEnumObjectImpl)cacheObject);
                        }
                        else {
                            Object obj = source.getObject();

                            if (obj instanceof BinaryEnumObjectImpl)
                                return fromBinaryEnum(ctx, targetType, (BinaryEnumObjectImpl)obj);

                            if (obj instanceof Enum)
                                return fromEnum(ctx, targetType, (Enum)obj);
                        }
                }
            }
            catch (IgniteCheckedException ex) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, ex);
            }
        }

        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "type:" + targetType);
    }

    /**
     * Converts {@link org.h2.value.Value} to specified type.
     *
     * @param ctx Kernal context.
     * @param source Source value.
     * @param targetType Target value type.
     * @return Result.
     */
    public Value convertValue(GridKernalContext ctx, Value source, int targetType) {
        if (dataTypesById.get(targetType) != null)
            return convert(ctx, source, targetType);

        return source.convertTo(targetType);
    }

    /**
     * Checks if type identifier is registered.
     *
     * @param typeId Type identifier.
     * @return
     */
    public boolean isRegistered(int typeId) {
        return dataTypesById.containsKey(typeId);
    }

    /**
     * Finds data type name given type id.
     *
     * @param typeId
     * @return Data type name or {@code null} if not registered.
     */
    public String findDataTypeName(int typeId) {
        EnumDataType dataType = dataTypesById.get(typeId);
        if (dataType == null)
            return null;

        return dataType.name;
    }

    /**
     * Wraps object to respective enum {@link Value}.
     *
     * @param obj Object to convert.
     * @param type type identifier.
     * @return H2 Value.
     * @throws IgniteCheckedException
     */
    public Value wrap(CacheObjectContext cctx, Object obj, int type, boolean check) throws IgniteCheckedException {
        if (dataTypesById.get(type) == null) {
            if (check)
                return null;
            else
                throw new IgniteCheckedException("Failed to wrap value[type=" +
                        type + ", value=" + obj + "]");
        }

        if (obj instanceof KeyCacheObjectImpl)
            obj = ((KeyCacheObjectImpl)obj).value(cctx, false);

        if (obj instanceof CacheObjectImpl)
            obj = ((CacheObjectImpl)obj).value(cctx, false);

        if (obj instanceof BinaryEnumObjectImpl)
            return fromBinaryEnum(cctx.kernalContext(), type, (BinaryEnumObjectImpl)obj);

        if (obj instanceof Enum)
            return fromEnum(cctx.kernalContext(), type, (Enum)obj);

        return getValue(type, obj, null);
    }

    /**
     * Gets Enum Value given the ordinal constant.
     *
     * @param type Data type id.
     * @param ord Enum ordinal value.
     * @return Enum Value.
     */
    private Value fromOrdinal(GridKernalContext ctx, int type, int ord) {
        return new GridH2ValueEnum(ctx, type, ord, null, null);
    }

    /**
     * Gets Enum Value given the name of enum constant.
     *
     * @param type Data type id.
     * @param name Enum constant's name.
     * @return Enum Value.
     * @throws IgniteCheckedException
     */
    private Value fromName(GridKernalContext ctx, int type, String name) throws IgniteCheckedException {
        BinaryMetadata binMeta = ((CacheObjectBinaryProcessorImpl)ctx.cacheObjects()).metadata0(type);
        Integer ord = binMeta.getEnumOrdinalByName(name);
        if (ord == null)
            throw new IgniteCheckedException("Unable to resolve enum constant ordinal [typeId=" +
                type + ", typeName='" + binMeta.typeName() + "', name='" + name + "']");
        return new GridH2ValueEnum(ctx, type, ord, name, null);
    }

    /**
     * Gets Enum Value provided the Enum object itself.
     * @param type Data type id.
     * @param obj Enum object.
     * @return Enum Value.
     * @throws IgniteCheckedException
     */
    private Value fromEnum(GridKernalContext ctx, int type, Enum obj) throws IgniteCheckedException {
        if (ctx.cacheObjects().binary().typeId(obj.getClass().getName()) == type)
            return new GridH2ValueEnum(ctx, type, obj.ordinal(), obj.name(), null);

        throw new IgniteCheckedException("Cannot convert enum + '" + obj.getClass().getName() +
                "' to value data type with id " + type);
    }

    /**
     * Gets Enum Value provided the Binary Enum object.
     * @param type Data type id.
     * @param binEnum Binary enum.
     * @return Enum Value.
     * @throws IgniteCheckedException
     */
    private Value fromBinaryEnum(GridKernalContext ctx, int type, BinaryEnumObjectImpl binEnum) throws IgniteCheckedException {
        if (binEnum.typeId() == type)
            return new GridH2ValueEnum(ctx, type, binEnum.enumOrdinal(), null, binEnum);

        throw new IgniteCheckedException("Cannot convert binary enum with type id " + binEnum.typeId() +
                " to value with data type id " + type);
    }

    /** Enum Data Type Descriptor */
    private class EnumDataType extends DataType {
        /** Enum class name */
        private String className;

        /**
         * Enum data type descriptor constructor.
         * @param typeId Type identifier.
         * @param dataTypeName Data type name reported to H2.
         * @param className Class name.
         */
        EnumDataType(int typeId, String dataTypeName, String className) {
            super.type = typeId;
            super.sqlType = Types.JAVA_OBJECT;
            super.name = dataTypeName;
            this.className = className;
        }
    }
}