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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.type;

import java.io.Serializable;
import java.util.LinkedHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Serializable RelDataType representation.
 */
public interface DataType extends Serializable {
    /**
     * Factory method to construct data type representation from RelDataType.
     * @param type RelDataType.
     * @return DataType.
     */
    static DataType fromType(RelDataType type) {
        if (type.isStruct()) {
            assert type.isStruct();

            LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();

            for (RelDataTypeField field : type.getFieldList())
                fields.put(field.getName(), fromType(field.getType()));

            return new StructType(fields);
        }

        if (type instanceof RelDataTypeFactoryImpl.JavaType)
            return new JavaType(((RelDataTypeFactoryImpl.JavaType) type).getJavaClass(), type.isNullable());

        assert type instanceof BasicSqlType : type;

        if (SqlTypeUtil.inCharFamily(type))
            return new CharacterType((BasicSqlType) type);

        return new BasicType((BasicSqlType) type);
    }

    /** */
    SqlTypeName typeName();

    /** */
    int precision();

    /** */
    int scale();

    /** */
    boolean nullable();

    /**
     * Returns logical type.
     *
     * @param factory Type factory.
     * @return Logical type.
     */
    RelDataType logicalType(IgniteTypeFactory factory);

    /**
     * Returns java type.
     *
     * @param factory Type factory.
     * @return Java type.
     */
    Class<?> javaType(IgniteTypeFactory factory);
}
