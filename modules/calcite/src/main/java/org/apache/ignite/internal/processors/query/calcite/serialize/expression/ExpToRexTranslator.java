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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A translator of Expressions into Rex nodes.
 */
public class ExpToRexTranslator implements ExpImplementor<RexNode> {
    /** */
    private final RexBuilder builder;

    /** */
    private final Map<Pair<String, SqlSyntax>, SqlOperator> ops;

    /**
     * Creates a Translator.
     *
     * @param builder Rex builder.
     * @param opTable Operators table.
     */
    public ExpToRexTranslator(RexBuilder builder, SqlOperatorTable opTable) {
        this.builder = builder;

        List<SqlOperator> opList = opTable.getOperatorList();

        HashMap<Pair<String, SqlSyntax>, SqlOperator> ops = U.newHashMap(opList.size());

        for (SqlOperator op : opList)
            ops.put(Pair.of(op.getName(), op.getSyntax()), op);

        this.ops = ops;
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
        return exp.implement(this);
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(CallExpression exp) {
        return builder.makeCall(exp.dataType().toRelDataType(builder.getTypeFactory()), op(exp.name(), exp.syntax()), translate(exp.operands()));
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(InputRefExpression exp) {
        return builder.makeInputRef(exp.dataType().toRelDataType(builder.getTypeFactory()), exp.index());
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(LiteralExpression exp) {
        return builder.makeLiteral(exp.value(), exp.dataType().toRelDataType(builder.getTypeFactory()), false);
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(LocalRefExpression exp) {
        return new RexLocalRef(exp.index(), exp.dataType().toRelDataType(builder.getTypeFactory()));
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(DynamicParamExpression exp) {
        return builder.makeDynamicParam(exp.dataType().toRelDataType(builder.getTypeFactory()), exp.index());
    }

    /** {@inheritDoc} */
    @Override public RexNode implement(Expression exp) {
        return exp.implement(this);
    }

    /** */
    private SqlOperator op(String name, SqlSyntax syntax) {
        return ops.get(Pair.of(name, syntax));
    }
}
