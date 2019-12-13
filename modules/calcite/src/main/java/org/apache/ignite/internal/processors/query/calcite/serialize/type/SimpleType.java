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

package org.apache.ignite.internal.processors.query.calcite.serialize.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 *
 */
public class SimpleType implements DataType {
    private final Class clazz;
    private final SqlTypeName typeName;
    private final int precision;
    private final int scale;

    public static SimpleType fromType(RelDataType type) {
        assert !type.isStruct();

        if (type instanceof RelDataTypeFactoryImpl.JavaType)
            return new SimpleType(((RelDataTypeFactoryImpl.JavaType) type).getJavaClass(), null, 0, 0);

        return new SimpleType(null, type.getSqlTypeName(), type.getPrecision(), type.getScale());
    }

    private SimpleType(Class clazz, SqlTypeName typeName, int precision, int scale) {
        this.clazz = clazz;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
    }

    @Override public RelDataType toRelDataType(RelDataTypeFactory factory) {
        if (clazz != null)
            return factory.createJavaType(clazz);
        if (typeName.allowsNoPrecNoScale())
            return factory.createSqlType(typeName);
        if (typeName.allowsPrecNoScale())
            return factory.createSqlType(typeName, precision);

        return factory.createSqlType(typeName, precision, scale);
    }
}
