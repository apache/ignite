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

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Describes {@link org.apache.calcite.rex.RexCall}.
 */
public class Call implements Expression {
    /** */
    private DataType type;

    /** */
    private String opName;

    /** */
    private SqlSyntax syntax;

    /** */
    private List<Expression> operands;

    /** */
    private transient CallOperation opImpl;

    /**
     * @param type Result type.
     * @param operands Operands.
     */
    public Call(DataType type, String opName, SqlSyntax syntax, List<Expression> operands) {
        this.type = type;
        this.opName = opName;
        this.syntax = syntax;
        this.operands = operands;
    }

    /** {@inheritDoc} */
    @Override public DataType resultType() {
        return type;
    }

    /**
     * @param opTable Operators table.
     * @return Sql logical operator.
     */
    public SqlOperator sqlOperator(SqlOperatorTable opTable) {
        List<SqlOperator> bag = new MutableSingletonList<>();

        opTable.lookupOperatorOverloads(
            new SqlIdentifier(opName, SqlParserPos.ZERO), null,
            syntax, bag, SqlNameMatchers.withCaseSensitive(true));

        return Objects.requireNonNull(F.first(bag));
    }

    /** */
    public String opName() {
        return opName;
    }

    /** */
    public SqlSyntax syntax() {
        return syntax;
    }

    /**
     * @return Operands.
     */
    public List<Expression> operands() {
        return operands;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public <T> T evaluate(ExecutionContext ctx, Object... args) {
        if (opImpl == null) {
            IgniteTypeFactory typeFactory = ctx.parent().typeFactory();
            SqlConformance conformance = ctx.parent().conformance();
            SqlOperatorTable opTable = ctx.parent().opTable();

            opImpl = new ExpressionFactory(typeFactory, conformance, opTable).implement(this);
        }

        return (T) opImpl.apply(operands.stream().map(o -> o.evaluate(ctx, args)).toArray());
    }
}
