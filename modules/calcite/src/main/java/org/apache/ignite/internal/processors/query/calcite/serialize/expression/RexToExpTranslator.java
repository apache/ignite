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
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

/**
 *
 */
public class RexToExpTranslator implements RexVisitor<Expression> {
    public List<Expression> translate(List<RexNode> operands) {
        ArrayList<Expression> res = new ArrayList<>(operands.size());

        for (RexNode operand : operands) {
            res.add(translate(operand));
        }

        return res;
    }

    public Expression translate(RexNode rex) {
        return rex.accept(this);
    }

     @Override public Expression visitInputRef(RexInputRef inputRef) {
        return new InputRefExpression(inputRef.getType(), inputRef.getIndex());
    }

    @Override public Expression visitLocalRef(RexLocalRef localRef) {
        return new LocalRefExpression(localRef.getType(), localRef.getIndex());
    }

    @Override public Expression visitLiteral(RexLiteral literal) {
        return new LiteralExpression(literal.getType(), literal.getValue());
    }

    @Override public Expression visitCall(RexCall call) {
        return new CallExpression(call.getOperator(), translate(call.getOperands()));
    }

    @Override public Expression visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        return new DynamicParamExpression(dynamicParam.getType(), dynamicParam.getIndex());
    }

    @Override public Expression visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    @Override public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException();
    }
}
