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

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.SystemType;

/**
 *
 */
public final class Types {
    /** */
    private Types() {
        // No-op.
    }

    /**
     * Factory method to construct data type representation from RelDataType.
     * @param type RelDataType.
     * @return DataType.
     */
    public static DataType fromType(RelDataType type) {
        if (type.isStruct()) {
            assert type.isStruct();

            LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();

            for (RelDataTypeField field : type.getFieldList())
                fields.put(field.getName(), fromType(field.getType()));

            return new StructType(fields);
        }

        if (type instanceof RelDataTypeFactoryImpl.JavaType)
            return new JavaType(((RelDataTypeFactoryImpl.JavaType) type).getJavaClass(), type instanceof SystemType);

        assert type instanceof org.apache.calcite.sql.type.BasicSqlType : type;

        if (SqlTypeUtil.inCharFamily(type))
            return new CharacterSqlType(type.getSqlTypeName(),
                type.getCharset(),
                type.getCollation(),
                type.getPrecision(),
                type.getScale(),
                type.isNullable()
            );

        return new BasicSqlType(type.getSqlTypeName(), type.getPrecision(), type.getScale(), type.isNullable());
    }

    /** */
    private static class JavaType implements DataType {
        /** */
        private final Class<?> clazz;

        /** */
        private final boolean system;

        /**
         * @param clazz Value class.
         * @param system System type flag.
         */
        private JavaType(Class<?> clazz, boolean system) {
            this.clazz = clazz;
            this.system = system;
        }

        /** {@inheritDoc} */
        @Override public RelDataType toRelDataType(IgniteTypeFactory factory) {
            return system ? factory.createSystemType(clazz) : factory.createJavaType(clazz);
        }
    }

    /** */
    private static class BasicSqlType implements DataType {
        /** */
        private final SqlTypeName typeName;

        /** */
        private final int precision;

        /** */
        private final int scale;

        /** */
        private final boolean nullable;

        /**
         * @param typeName Type name
         * @param precision Precision.
         * @param scale Scale.
         * @param nullable Nullable flag.
         */
        private BasicSqlType(SqlTypeName typeName, int precision, int scale, boolean nullable) {
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.nullable = nullable;
        }

        /** {@inheritDoc} */
        @Override public RelDataType toRelDataType(IgniteTypeFactory factory) {
            RelDataType type;

            if (!typeName.allowsPrec())
                type = factory.createSqlType(typeName);
            else if (!typeName.allowsScale())
                type = factory.createSqlType(typeName, precision);
            else
                type = factory.createSqlType(typeName, precision, scale);

            return nullable ? factory.createTypeWithNullability(type, nullable) : type;
        }
    }

    /** */
    private static class CharacterSqlType extends BasicSqlType {
        /** */
        private final Charset charset;

        /** */
        private final SqlCollation collation;

        /**
         * @param typeName  Type name
         * @param charset Charset.
         * @param collation Collation.
         * @param precision Precision.
         * @param scale     Scale.
         * @param nullable Nullable flag.
         */
        private CharacterSqlType(SqlTypeName typeName, Charset charset, SqlCollation collation, int precision, int scale, boolean nullable) {
            super(typeName, precision, scale, nullable);
            this.charset = charset;
            this.collation = collation;
        }

        /** {@inheritDoc} */
        @Override public RelDataType toRelDataType(IgniteTypeFactory factory) {
            return factory.createTypeWithCharsetAndCollation(super.toRelDataType(factory), charset, collation);
        }
    }

    /** */
    private static class StructType implements DataType {
        /** */
        private final LinkedHashMap<String, DataType> fields;

        /**
         * @param fields Fields.
         */
        private StructType(LinkedHashMap<String, DataType> fields) {
            this.fields = fields;
        }

        /** {@inheritDoc} */
        @Override public RelDataType toRelDataType(IgniteTypeFactory factory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
            fields.forEach((n,f) -> builder.add(n,f.toRelDataType(factory)));
            return builder.build();
        }
    }
}
