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

import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;

/**
 * Describes {@link org.apache.calcite.rex.RexLiteral}.
 */
public class Literal implements Expression {
    /** */
    private final DataType type;

    /** */
    private final Object value;

    /**
     * @param type Data type.
     * @param value Value.
     */
    public Literal(DataType type, Object value) {
        this.type = type;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override public DataType resultType() {
        return type;
    }

    /**
     * @return Value.
     */
    public Object value() {
        return value;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public <T> T evaluate(ExecutionContext ctx, Object... args) {
        return (T) value;
    }
}
