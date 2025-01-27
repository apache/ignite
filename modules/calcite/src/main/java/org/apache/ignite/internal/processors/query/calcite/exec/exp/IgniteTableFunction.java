/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteTableFunction extends ReflectiveFunctionBase implements TableFunction, ImplementableFunction {
    /** */
    private final CallImplementor implementor;

    /** */
    private final Class<?>[] columnTypes;

    /** */
    private final List<String> columnNames;

    /** */
    private IgniteTableFunction(Method method, Class<?>[] columnTypes, @Nullable List<String> columnNames,
        CallImplementor implementor) {
        super(method);

        validate(method, columnTypes, columnNames);

        this.implementor = implementor;
        this.columnTypes = columnTypes;

        this.columnNames = columnNames == null ? new ArrayList<>(columnTypes.length) : columnNames;

        if (F.isEmpty(columnNames)) {
            for (Class<?> cl : columnTypes)
                this.columnNames.add("COL_" + this.columnNames.size());
        }
    }

    /** */
    public static Function create(Method method, Class<?>[] tableColumnTypes) {
        CallImplementor impl = RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(method),
            NullPolicy.NONE, false);

        return new IgniteTableFunction(method, tableColumnTypes, null, impl);
    }

    /** {@inheritDoc} */
    @Override public CallImplementor getImplementor() {
        return implementor;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, List<?> arguments) {
        JavaTypeFactory tf = (JavaTypeFactory)typeFactory;

        List<RelDataType> converted = Stream.of(columnTypes).map(cl -> tf.toSql(tf.createType(cl))).collect(Collectors.toList());

        return typeFactory.createStructType(converted, columnNames);
    }

    /** {@inheritDoc} */
    @Override public Type getElementType(List<?> arguments) {
        // Calcite's {@link TableFunctionImpl} does a real invocation ({@link TableFunctionImpl#apply(List)}) to determine
        // the type. The call might be long, 'heavy' and should not be executed at validation/planning. We may check the
        // argument number here but not their types. The types might be wrong, but converted further.
        if (F.isEmpty(arguments) && !F.isEmpty(method.getParameterTypes())
            || F.isEmpty(method.getParameterTypes()) && !F.isEmpty(arguments)
            || method.getParameterTypes().length != arguments.size()) {
            throw new IllegalArgumentException("Wrong arguments number: " + arguments.size() + ". Expected: "
                + method.getParameterTypes().length + '.');
        }

        return Iterable.class;
    }

    /** */
    private static void validate(Method method, Class<?>[] columnTypes, List<String> columnNames) {
        if (F.isEmpty(columnTypes))
            throw new IllegalArgumentException("Column types of the table cannot be empty.");

        if (!F.isEmpty(columnNames) && columnTypes.length != columnNames.size()) {
            throw new IllegalArgumentException("Number of the table column names [" + columnNames.size() + "] must either " +
                "be empty or match number of the column types [" + columnTypes.length + "].");
        }

        if (!Iterable.class.isAssignableFrom(method.getReturnType()))
            throw new IllegalArgumentException("The method is expected to return a collection (iteratable).");
    }
}
