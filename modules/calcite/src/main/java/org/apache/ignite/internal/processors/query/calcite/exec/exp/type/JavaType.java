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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/** */
public class JavaType implements DataType {
    /** */
    private final Class<?> clazz;

    /** */
    private final boolean nullable;

    /** {@inheritDoc} */
    @Override public SqlTypeName typeName() {
        final SqlTypeName typeName =
            JavaToSqlTypeConversionRules.instance().lookup(clazz);

        return typeName == null ? SqlTypeName.OTHER : typeName;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return PRECISION_NOT_SPECIFIED;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return SCALE_NOT_SPECIFIED;
    }

    /** {@inheritDoc} */
    @Override public boolean nullable() {
        return nullable;
    }

    /**
     * @param clazz Value class.
     * @param nullable nullable flag.
     */
    public JavaType(Class<?> clazz, boolean nullable) {
        assert !nullable || !clazz.isPrimitive();

        this.clazz = clazz;
        this.nullable = nullable;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory factory) {
        return factory.createJavaType(clazz);
    }

    /** {@inheritDoc} */
    @Override public Class<?> javaType(IgniteTypeFactory factory) {
        return clazz;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(clazz);
    }
}
