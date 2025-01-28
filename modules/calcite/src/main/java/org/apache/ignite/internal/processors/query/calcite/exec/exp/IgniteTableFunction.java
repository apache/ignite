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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TableFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Holder of user-defined table function.
 *
 * @see QuerySqlFunction#tableColumnTypes()
 */
public class IgniteTableFunction extends IgniteReflectiveFunctionBase implements TableFunction {
    /** Column types of the returned table representation. */
    private final Class<?>[] colTypes;

    /** Column names of the returned table representation. */
    private final List<String> colNames;

    /**
     * Creates user-defined table function holder.
     *
     * @param method The implementation method.
     * @param colTypes Column types of the returned table representation.
     * @param colNames Column names of the returned table representation. Of empty, the defaults are used ('COL_1', 'COL_2', etc.).
     * @param implementor Call implementor.
     */
    private IgniteTableFunction(Method method, Class<?>[] colTypes, @Nullable String[] colNames, CallImplementor implementor) {
        super(method, implementor);

        validate(method, colTypes, colNames);

        this.colTypes = colTypes;

        this.colNames = F.isEmpty(colNames) ? new ArrayList<>(colTypes.length) : Arrays.asList(colNames);

        if (F.isEmpty(colNames)) {
            for (int i = 0; i < colTypes.length; ++i)
                this.colNames.add("COL_" + (i + 1));
        }
    }

    /**
     * Creates user-defined table function implementor and holder.
     *
     * @param method The implementation method.
     * @param colTypes Column types of the returned table representation.
     * @param colNames Column names of the returned table representation. Of empty, the defaults are used ('COL_1', 'COL_2', etc.).
     */
    public static IgniteTableFunction create(Method method, Class<?>[] colTypes, @Nullable String[] colNames) {
        CallImplementor impl = RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);

        return new IgniteTableFunction(method, colTypes, colNames, impl);
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
        // the type. The call might be long, 'heavy', affect some metrics and should not be executed at validation/planning.
        // We may check the argument number here but not their types. The types might be wrong, but converted further.
        if (F.isEmpty(arguments) && !F.isEmpty(method.getParameterTypes())
            || F.isEmpty(method.getParameterTypes()) && !F.isEmpty(arguments)
            || method.getParameterTypes().length != arguments.size()) {
            throw new IllegalArgumentException("Wrong arguments number: " + arguments.size() + ". Expected: "
                + method.getParameterTypes().length + '.');
        }

        return Iterable.class;
    }

    /** Validates the parameters and throws an exception if finds an incorrect parameter. */
    private static void validate(Method mtd, Class<?>[] colTypes, String[] colNames) {
        if (F.isEmpty(colTypes))
            raiseValidationError(mtd, "Column types cannot be empty.");

        if (!F.isEmpty(colNames)) {
            if (colTypes.length != colNames.length) {
                raiseValidationError(mtd, "Number of the table column names [" + colNames.length
                    + "] must either be empty or match the number of column types [" + colTypes.length + "].");
            }

            if (new HashSet<>(Arrays.asList(colNames)).size() != colNames.length)
                raiseValidationError(mtd, "One or more column names is not unique.");
        }

        if (!Iterable.class.isAssignableFrom(mtd.getReturnType()))
            raiseValidationError(mtd, "The method is expected to return a collection (iterable).");
    }

    /**
     * Throws a parameter validation exception with a standard text prefix.
     *
     * @param method Java-method of the related user-defined table function.
     * @param errPostfix Error text postfix.
     */
    private static void raiseValidationError(Method method, String errPostfix) {
        String mtdSign = method.getName() + '(' + Stream.of(method.getParameterTypes()).map(Class::getSimpleName)
            .collect(Collectors.joining(", ")) + ')';

        throw new IgniteSQLException("Unable to create table function for method '" + mtdSign + "'. " + errPostfix);
    }
}
