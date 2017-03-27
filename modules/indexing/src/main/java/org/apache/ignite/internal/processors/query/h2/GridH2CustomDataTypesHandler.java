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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnum;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnumCache;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.api.CustomDataTypesHandler;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.util.JdbcUtils;
import org.h2.value.ValueJavaObject;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedHashMap;

import java.sql.Types;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Custom data types handler implementation
 */
public class GridH2CustomDataTypesHandler implements CustomDataTypesHandler {
    private ConcurrentMap<Integer, Class<?>> classById = new ConcurrentHashMap8<>();
    private ConcurrentMap<Class<?>, Integer> idByClass = new ConcurrentHashMap8<>();

    private ConcurrentMap<Integer, DataType> dataTypesById = new ConcurrentHashMap8<>();
    private ConcurrentMap<String, DataType> dataTypesByName = new ConcurrentHashMap8<>();

    private final static int INVALID_TYPE_ID_RANGE_BEGIN = -1;
    private final static int INVALID_TYPE_ID_RANGE_END = 100;
    private final static int ENUM_ORDER = 100_000;

    /** */
    public String registerEnum(int typeId, Class<?> cls, String alias) {
        assert cls.isEnum();

        String name = IgniteH2Indexing.escapeName(
                F.isEmpty(alias)? cls.getName() : alias, false).toUpperCase();

        if (dataTypesByName.containsKey(name))
            return name;

        if ((INVALID_TYPE_ID_RANGE_BEGIN <= typeId) && (typeId <= INVALID_TYPE_ID_RANGE_END))
            throw new IgniteException("Enums with type id in range [" + INVALID_TYPE_ID_RANGE_BEGIN +
                    ", " + INVALID_TYPE_ID_RANGE_END + "] are prohibited");

        DataType dataType = new DataType();
        dataType.type = typeId;
        dataType.sqlType = Types.JAVA_OBJECT;
        dataType.name = name;

        idByClass.put(cls, typeId);
        classById.put(typeId, cls);
        dataTypesById.put(typeId, dataType);
        dataTypesByName.put(name, dataType);

        return name;
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
        Class<?> cls = classById.get(type);
        if (cls == null)
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type: " + type);
        return cls.getName();
    }

    /** {@inheritDoc} */
    @Override public int getTypeIdFromClass(Class<?> cls) {
        Integer result = idByClass.get(cls);
        if (result == null)
            return Value.JAVA_OBJECT;
        return result;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int type, Object data, DataHandler dataHandler) {
        Integer typeId = type;

        if (type == Value.UNKNOWN)
            typeId = idByClass.get(data.getClass());

        if (typeId == null)
            return ValueJavaObject.getNoCopy(data, null, dataHandler);

        Class<?> cls = classById.get(typeId);

        if (cls != null) {
            try {
                if (data instanceof Integer)
                    return GridH2ValueEnumCache.get(typeId, cls, (Integer) data);

                if (data instanceof String)
                    return GridH2ValueEnumCache.get(typeId, cls, (String) data);

                if (data instanceof Enum)
                    return GridH2ValueEnumCache.get(typeId, cls, ((Enum) data).ordinal());
            } catch (IgniteCheckedException ex) {
                //fallthrough
            }
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
        if (source.getType() == targetType)
            return source;

        if (source instanceof GridH2ValueEnum)
            return source.convertTo(targetType);

        Class<?> cls = classById.get(targetType);

        if (cls != null) {
            try {
                switch (source.getType()) {
                    case Value.INT:
                        return GridH2ValueEnumCache.get(targetType, cls, source.getInt());
                    case Value.STRING:
                        return GridH2ValueEnumCache.get(targetType, cls, source.getString());
                    case Value.JAVA_OBJECT:
                        Object obj = JdbcUtils.deserialize(source.getBytesNoCopy(), null);
                        if (obj instanceof Enum)
                            return GridH2ValueEnumCache.get(targetType, cls, ((Enum) obj).ordinal());
                }
            }
            catch (IgniteCheckedException ex) {
                // fallthrough
            }
        }

        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "type:" + targetType);
    }
}
