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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 *
 */
public class CallExpression implements Expression {
    private final RelDataType type;
    private final SqlOperator op;
    private final List<Expression> operands;

    public CallExpression(RelDataType type, SqlOperator op, List<Expression> operands) {
        this.type = type;
        this.op = op;
        this.operands = operands;
    }

    @Override public RexNode toRex(RexBuilder builder) {
        ArrayList<RexNode> operands0 = new ArrayList<>(operands.size());

        for (Expression operand : operands) {
            operands0.add(operand.toRex(builder));
        }

        return builder.makeCall(type, op, operands0);
    }
}
