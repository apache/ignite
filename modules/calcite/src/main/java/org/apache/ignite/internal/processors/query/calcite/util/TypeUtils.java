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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.NotNull;

/** */
public class TypeUtils {
    /** */
    public static RelDataType combinedRowType(IgniteTypeFactory typeFactory, RelDataType... types) {

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        Set<String> names = new HashSet<>();

        for (RelDataType type : types) {
            for (RelDataTypeField field : type.getFieldList()) {
                int idx = 0;
                String fieldName = field.getName();

                while (!names.add(fieldName))
                    fieldName = field.getName() + idx++;

                builder.add(fieldName, field.getType());
            }
        }

        return builder.build();
    }

    /** */
    public static boolean needCast(RelDataTypeFactory factory, RelDataType fromType, RelDataType toType) {
        // This prevents that we cast a JavaType to normal RelDataType.
        if (fromType instanceof RelDataTypeFactoryImpl.JavaType
            && toType.getSqlTypeName() == fromType.getSqlTypeName()) {
            return false;
        }

        // Do not make a cast when we don't know specific type (ANY) of the origin node.
        if (toType.getSqlTypeName() == SqlTypeName.ANY
            || fromType.getSqlTypeName() == SqlTypeName.ANY) {
            return false;
        }

        // No need to cast between char and varchar.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType))
            return false;

        // No need to cast if the source type precedence list
        // contains target type. i.e. do not cast from
        // tinyint to int or int to bigint.
        if (fromType.getPrecedenceList().containsType(toType)
            && SqlTypeUtil.isIntType(fromType)
            && SqlTypeUtil.isIntType(toType)) {
            return false;
        }

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(factory, fromType, toType))
            return false;

        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);

        return true;
    }

    /** */
    @NotNull public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull Class<?>... fields) {
        List<RelDataType> types = Arrays.stream(fields)
            .map(typeFactory::createJavaType)
            .collect(Collectors.toList());

        return createRowType(typeFactory, types, "$F");
    }

    /** */
    @NotNull public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull RelDataType... fields) {
        List<RelDataType> types = Arrays.asList(fields);

        return createRowType(typeFactory, types, "$F");
    }

    /** */
    private static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields, String namePreffix) {
        List<String> names = IntStream.range(0, fields.size())
            .mapToObj(ord -> namePreffix + ord)
            .collect(Collectors.toList());

        return typeFactory.createStructType(fields, names);
    }
}
