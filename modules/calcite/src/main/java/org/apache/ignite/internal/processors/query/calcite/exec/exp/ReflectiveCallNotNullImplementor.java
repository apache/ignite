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
import java.lang.reflect.Type;
import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;

import static org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod.UDF_INSTANCE;

/**
 * Implementation of {@link NotNullImplementor} that calls a given {@link Method}.
 *
 * <p>When method is not static, a new instance of the required class is
 * created.
 */
public class ReflectiveCallNotNullImplementor implements NotNullImplementor {
    /** */
    protected final Method method;

    /**
     * Constructor of ReflectiveCallNotNullImplementor.
     *
     * @param method Method that is used to implement the call
     */
    public ReflectiveCallNotNullImplementor(Method method) {
        this.method = method;
    }

    /** {@inheritDoc} */
    @Override public Expression implement(RexToLixTranslator translator,
        RexCall call, List<Expression> translatedOperands) {
        translatedOperands =
            ConverterUtils.fromInternal(translator, method.getParameterTypes(), translatedOperands);
        translatedOperands =
            ConverterUtils.convertAssignableTypes(method.getParameterTypes(), translatedOperands);
        Expression callExpr;
        if ((method.getModifiers() & Modifier.STATIC) != 0)
            callExpr = Expressions.call(method, translatedOperands);

        else {
            final Expression target = Expressions.convert_(
                Expressions.call(
                    translator.getRoot(),
                    UDF_INSTANCE.method(),
                    Expressions.constant(method.getDeclaringClass().getName())),
                method.getDeclaringClass());

            callExpr = Expressions.call(target, method, translatedOperands);
        }

        if (TypeUtils.isConvertableType(method.getReturnType())) {
            Type targetType = translator.typeFactory.getJavaClass(call.getType());
            Expression result = method.getReturnType().isPrimitive() ? Expressions.box(callExpr) : callExpr;

            callExpr = Expressions.convert_(
                Expressions.call(TypeUtils.class, "toInternal", translator.getRoot(), result),
                targetType
            );
        }

        if (!containsCheckedException(method))
            return callExpr;

        return translator.handleMethodCheckedExceptions(callExpr);
    }

    /** */
    private boolean containsCheckedException(Method method) {
        Class[] exceptions = method.getExceptionTypes();
        if (exceptions == null || exceptions.length == 0)
            return false;

        for (Class clazz : exceptions) {
            if (!RuntimeException.class.isAssignableFrom(clazz))
                return true;
        }
        return false;
    }
}
