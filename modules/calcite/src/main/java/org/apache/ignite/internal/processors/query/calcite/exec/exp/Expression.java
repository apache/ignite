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

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.io.Serializable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Describes {@link org.apache.calcite.rex.RexNode}
 */
public interface Expression extends Serializable {
    /**
     * Accepts a visit from a visitor.
     *
     * @param visitor Expression visitor.
     * @return Visit result.
     */
    <T> T accept(ExpressionVisitor<T> visitor);

    /** */
    DataType resultType();

    /** */
    default RelDataType logicalType(IgniteTypeFactory typeFactory) {
        return resultType().logicalType(typeFactory);
    }

    /** */
    default Class<?> javaType(IgniteTypeFactory typeFactory) {
        return resultType().javaType(typeFactory);
    }

    /**
     * Evaluates expression.
     *
     * @param ctx Execution context.
     * @return Evaluated value.
     */
    <T> T evaluate(ExecutionContext ctx, Object... args);
}
