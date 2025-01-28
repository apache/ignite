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
import java.util.Arrays;
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
    private final CallImplementor impl;

    /** */
    private final Class<?>[] colTypes;

    /** */
    @Nullable private final List<String> colNames;

    /** */
    private IgniteTableFunction(Method mtd, Class<?>[] colTypes, @Nullable String[] colNames, CallImplementor impl) {
        super(mtd);

        validate(mtd, colTypes, colNames);

        this.impl = impl;
        this.colTypes = colTypes;

        this.colNames = F.isEmpty(colNames) ? new ArrayList<>(colTypes.length) : Arrays.asList(colNames);

        if (F.isEmpty(colNames)) {
            for (int i = 0; i < colTypes.length; ++i)
                this.colNames.add("COL_" + i);
        }
    }

    /** */
    public static Function create(Method mtd, Class<?>[] colTypes, @Nullable String[] colNames) {
        CallImplementor impl = RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(mtd), NullPolicy.NONE, false);

        return new IgniteTableFunction(mtd, colTypes, colNames, impl);
    }

    /** {@inheritDoc} */
    @Override public CallImplementor getImplementor() {
        return impl;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, List<?> arguments) {
        JavaTypeFactory tf = (JavaTypeFactory)typeFactory;

        List<RelDataType> converted = Stream.of(colTypes).map(cl -> tf.toSql(tf.createType(cl))).collect(Collectors.toList());

        return typeFactory.createStructType(converted, colNames);
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
    private static void validate(Method mtd, Class<?>[] colTypes, String[] colNames) {
        if (F.isEmpty(colTypes))
            throw new IllegalArgumentException("Column types of the table cannot be empty.");

        if (!F.isEmpty(colNames) && colTypes.length != colNames.length) {
            throw new IllegalArgumentException("Number of the table column names [" + colNames.length + "] must either " +
                "be empty or match number of the column types [" + colTypes.length + "].");
        }

        if (!Iterable.class.isAssignableFrom(mtd.getReturnType()))
            throw new IllegalArgumentException("The method is expected to return a collection (iteratable).");
    }
}
