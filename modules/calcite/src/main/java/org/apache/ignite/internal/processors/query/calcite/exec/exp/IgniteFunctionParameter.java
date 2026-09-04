/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.OtherType;

/**
 * Reflective Java function parameter represented with a SQL type.
 *
 * <p>The SQL representation is required to validate user-defined function arguments and to convert literal arguments
 * while deriving a table function row type.
 */
final class IgniteFunctionParameter implements FunctionParameter {
    /** Original function parameter. */
    private final FunctionParameter delegate;

    /**
     * Constructor.
     *
     * @param delegate Original function parameter.
     */
    private IgniteFunctionParameter(FunctionParameter delegate) {
        this.delegate = delegate;
    }

    /** Returns function parameters represented with SQL types. */
    static List<FunctionParameter> toSql(List<FunctionParameter> parameters) {
        return parameters.stream().map(IgniteFunctionParameter::toSql).toList();
    }

    /** Returns a function parameter represented with a SQL type. */
    static FunctionParameter toSql(FunctionParameter parameter) {
        return new IgniteFunctionParameter(parameter);
    }

    /** {@inheritDoc} */
    @Override public int getOrdinal() {
        return delegate.getOrdinal();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.getName();
    }

    /** {@inheritDoc} */
    @Override public RelDataType getType(RelDataTypeFactory typeFactory) {
        JavaTypeFactory tf = (JavaTypeFactory)typeFactory;
        RelDataType type = tf.toSql(delegate.getType(typeFactory));

        // Prevent the validator from replacing OTHER with a structured type derived from a dynamic parameter value.
        return type.getSqlTypeName() == SqlTypeName.OTHER ? new OtherType(type.isNullable()) : type;
    }

    /** {@inheritDoc} */
    @Override public boolean isOptional() {
        return delegate.isOptional();
    }
}
