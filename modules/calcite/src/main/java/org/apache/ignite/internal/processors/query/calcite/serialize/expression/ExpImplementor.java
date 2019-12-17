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

package org.apache.ignite.internal.processors.query.calcite.serialize.expression;

/**
 * Implements Expression tree recursively using Visitor pattern.
 */
public interface ExpImplementor<T> {
    /**
     * See {@link ExpImplementor#implement(Expression)}
     */
    T implement(CallExpression exp);

    /**
     * See {@link ExpImplementor#implement(Expression)}
     */
    T implement(InputRefExpression exp);

    /**
     * See {@link ExpImplementor#implement(Expression)}
     */
    T implement(LiteralExpression exp);

    /**
     * See {@link ExpImplementor#implement(Expression)}
     */
    T implement(LocalRefExpression exp);

    /**
     * See {@link ExpImplementor#implement(Expression)}
     */
    T implement(DynamicParamExpression exp);

    /**
     * Implements given expression.
     *
     * @param exp Expression.
     * @return Implementation result.
     */
    T implement(Expression exp);
}
