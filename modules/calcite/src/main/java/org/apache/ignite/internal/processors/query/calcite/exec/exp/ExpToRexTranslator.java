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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * A translator of Expressions into Rex nodes.
 */
public class ExpToRexTranslator implements ExpressionVisitor<RexNode> {
    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final SqlOperatorTable opTable;

    /** */
    private final RexBuilder builder;

    /**
     * Creates a Translator.
     *
     * @param typeFactory Ignite type factory.
     * @param opTable Operators table.
     */
    public ExpToRexTranslator(IgniteTypeFactory typeFactory, SqlOperatorTable opTable) {
        this.typeFactory = typeFactory;
        this.opTable = opTable;

        builder = new RexBuilder(typeFactory);
    }

    /**
     * Translates a list of expressions into a list of Rex nodes.
     *
     * @param exps List of expressions.
     * @return List of Rex nodes.
     */
    public List<RexNode> translate(List<Expression> exps) {
        return Commons.transform(exps, this::translate);
    }

    /**
     * Translates an expression into a RexNode.
     *
     * @param exp Expression.
     * @return RexNode.
     */
    public RexNode translate(Expression exp) {
        return exp.accept(this);
    }

    /** {@inheritDoc} */
    @Override public RexNode visit(InputRef exp) {
        return builder.makeInputRef(exp.logicalType(typeFactory), exp.index());
    }

    /** {@inheritDoc} */
    @Override public RexNode visit(Literal exp) {
        return builder.makeLiteral(exp.value(), exp.logicalType(typeFactory),false);
    }

    /** {@inheritDoc} */
    @Override public RexNode visit(DynamicParam exp) {
        return builder.makeDynamicParam(exp.logicalType(typeFactory), exp.index());
    }

    /** {@inheritDoc} */
    @Override public RexNode visit(Call exp) {
        return builder.makeCall(exp.logicalType(typeFactory), exp.sqlOperator(opTable), translate(exp.operands()));
    }

    /** {@inheritDoc} */
    @Override public RexNode visit(Expression exp) {
        return exp.accept(this);
    }
}
