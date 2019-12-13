/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class ExpToRexTranslator implements ExpImplementor<RexNode> {
    private final RexBuilder builder;
    private final RelDataTypeFactory typeFactory;
    private final Map<Pair<String, SqlSyntax>, SqlOperator> ops;

    public ExpToRexTranslator(RexBuilder builder, RelDataTypeFactory typeFactory, SqlOperatorTable opTable) {
        this.builder = builder;
        this.typeFactory = typeFactory;

        List<SqlOperator> opList = opTable.getOperatorList();

        HashMap<Pair<String, SqlSyntax>, SqlOperator> ops = new HashMap<>(opList.size());

        for (SqlOperator op : opList) {
            ops.put(Pair.of(op.getName(), op.getSyntax()), op);
        }

        this.ops = ops;
    }

    public List<RexNode> translate(List<Expression> exps) {
        if (F.isEmpty(exps))
            return Collections.emptyList();

        if (exps.size() == 1)
            return F.asList(translate(F.first(exps)));

        List<RexNode> res = new ArrayList<>(exps.size());

        for (Expression exp : exps) {
            res.add(exp.implement(this));
        }

        return res;
    }

    public RexNode translate(Expression exp) {
        return exp.implement(this);
    }

    @Override public RexNode implement(CallExpression exp) {
        return builder.makeCall(op(exp.opName, exp.opSyntax), translate(exp.operands));
    }

    @Override public RexNode implement(InputRefExpression exp) {
        return builder.makeInputRef(exp.type.toRelDataType(typeFactory), exp.index);
    }

    @Override public RexNode implement(LiteralExpression exp) {
        return builder.makeLiteral(exp.value, exp.type.toRelDataType(typeFactory), false);
    }

    @Override public RexNode implement(LocalRefExpression exp) {
        return new RexLocalRef(exp.index, exp.type.toRelDataType(typeFactory));
    }

    @Override public RexNode implement(DynamicParamExpression exp) {
        return builder.makeDynamicParam(exp.type.toRelDataType(typeFactory), exp.index);
    }

    private SqlOperator op(String name, SqlSyntax syntax) {
        return ops.get(Pair.of(name, syntax));
    }
}
