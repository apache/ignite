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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;

/**
 * Describes {@link org.apache.calcite.rex.RexCall}.
 */
public class CallExpression implements Expression {
    /** */
    private final String opName;

    /** */
    private final SqlSyntax opSyntax;

    /** */
    private DataType type;

    /** */
    private final List<Expression> operands;

    /**
     * @param op Sql operation.
     * @param type Result type.
     * @param operands Operands.
     */
    public CallExpression(SqlOperator op, RelDataType type, List<Expression> operands) {
        opName = op.getName();
        this.type = DataType.fromType(type);
        this.operands = operands;
        opSyntax = op.getSyntax();
    }

    /**
     * @return Operation name.
     */
    public String name() {
        return opName;
    }

    /**
     * @return Operation syntax;
     */
    public SqlSyntax syntax() {
        return opSyntax;
    }

    /**
     * @return Data type.
     */
    public DataType dataType() {
        return type;
    }

    /**
     * @return Operands.
     */
    public List<Expression> operands() {
        return operands;
    }

    /** {@inheritDoc} */
    @Override public <T> T implement(ExpImplementor<T> implementor) {
        return implementor.implement(this);
    }
}
