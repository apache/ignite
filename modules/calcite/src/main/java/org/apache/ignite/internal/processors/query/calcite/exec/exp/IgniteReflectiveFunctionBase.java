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
import java.util.List;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;

/** A base for outer java-method functions. */
abstract class IgniteReflectiveFunctionBase extends ReflectiveFunctionBase implements ImplementableFunction {
    /** */
    protected final CallImplementor implementor;

    /** */
    private final List<FunctionParameter> funcParams;

    /** */
    protected IgniteReflectiveFunctionBase(Method method, CallImplementor implementor) {
        super(method);

        this.implementor = implementor;

        funcParams = IgniteFunctionParameter.toSql(super.getParameters());
    }

    /** {@inheritDoc} */
    @Override public List<FunctionParameter> getParameters() {
        return funcParams;
    }

    /** {@inheritDoc} */
    @Override public CallImplementor getImplementor() {
        return implementor;
    }
}
