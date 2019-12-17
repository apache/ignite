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
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * A translator of Rex nodes into Expressions.
 */
public class RexToExpTranslator implements RexVisitor<Expression> {

    /**
     * Translates a list of Rex nodes into a list of expressions.
     *
     * @param operands List of Rex nodes.
     * @return List of expressions.
     */
    public List<Expression> translate(List<RexNode> operands) {
        return Commons.transform(operands, this::translate);
    }

    /**
     * Translates a RexNode into an expression.
     *
     * @param rex RexNode.
     * @return Expression.
     */
    public Expression translate(RexNode rex) {
        return rex.accept(this);
    }

    /** {@inheritDoc} */
     @Override public Expression visitInputRef(RexInputRef inputRef) {
        return new InputRefExpression(inputRef.getType(), inputRef.getIndex());
    }

    /** {@inheritDoc} */
    @Override public Expression visitLocalRef(RexLocalRef localRef) {
        return new LocalRefExpression(localRef.getType(), localRef.getIndex());
    }

    /** {@inheritDoc} */
    @Override public Expression visitLiteral(RexLiteral literal) {
        return new LiteralExpression(literal.getType(), literal.getValue());
    }

    /** {@inheritDoc} */
    @Override public Expression visitCall(RexCall call) {
        return new CallExpression(call.getOperator(), translate(call.getOperands()));
    }

    /** {@inheritDoc} */
    @Override public Expression visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        return new DynamicParamExpression(dynamicParam.getType(), dynamicParam.getIndex());
    }

    /** {@inheritDoc} */
    @Override public Expression visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException();
    }
}
