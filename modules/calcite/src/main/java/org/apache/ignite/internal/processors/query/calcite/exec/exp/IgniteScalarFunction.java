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
import java.lang.reflect.Modifier;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;

/**
 * Implementation of {@link ScalarFunction} for Ignite user defined functions.
 */
public class IgniteScalarFunction extends ReflectiveFunctionBase implements ScalarFunction, ImplementableFunction {
    /** Implementor. */
    private final CallImplementor implementor;

    /**
     * Private constructor.
     */
    private IgniteScalarFunction(Method method, CallImplementor implementor) {
        super(method);

        this.implementor = implementor;
    }

    /**
     * Creates {@link ScalarFunction} from given method.
     *
     * @param method Method that is used to implement the function.
     * @return Created {@link ScalarFunction}.
     */
    public static ScalarFunction create(Method method) {
        assert Modifier.isStatic(method.getModifiers());

        CallImplementor implementor = RexImpTable.createImplementor(
            new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);

        return new IgniteScalarFunction(method, implementor);
    }

    /** {@inheritDoc} */
    @Override public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(method.getReturnType());
    }

    /** {@inheritDoc} */
    @Override public CallImplementor getImplementor() {
        return implementor;
    }
}
