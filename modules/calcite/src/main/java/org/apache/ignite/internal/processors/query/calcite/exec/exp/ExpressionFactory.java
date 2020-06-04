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

import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode;

/**
 * Expression factory.
 */
public interface ExpressionFactory<Row> {
    /** */
    Supplier<List<AccumulatorWrapper<Row>>> accumulatorsFactory(AggregateNode.AggregateType type, List<AggregateCall> calls, RelDataType rowType);

    /**
     * Creates a comparator for given data type and collations. Mainly used for sorted exchange.
     *
     * @param collations Collations.
     * @return Row comparator.
     */
    Comparator<Row> comparator(RelCollation collations);

    /**
     * Creates a Filter predicate.
     * @param filter Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    Predicate<Row> predicate(RexNode filter, RelDataType rowType);

    /**
     * Creates a Project function. Resulting function returns a row with different fields,
     * fields order, fields types, etc.
     * @param projects Projection expressions.
     * @param rowType Input row type.
     * @return Project function.
     */
    Function<Row, Row> project(List<RexNode> projects, RelDataType rowType);

    /**
     * Creates a Values relational node rows source.
     *
     * @param values Values.
     * @param rowType Output row type.
     * @return Values relational node rows source.
     */
    Iterable<Row> values(List<RexLiteral> values, RelDataType rowType);

    /**
     * Creates row from RexNodes.
     *
     * @param values Values.
     * @return Row.
     */
    Row asRow(List<RexNode> values, RelDataType rowType);

    /**
     * Creates {@link Scalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return Scalar.
     */
    default Scalar scalar(RexNode node, RelDataType type) {
        return scalar(ImmutableList.of(node), type);
    }

    /**
     * Creates {@link Scalar}, a code-generated expressions evaluator.
     *
     * @param nodes Expressions.
     * @param type Row type.
     * @return Scalar.
     */
    Scalar scalar(List<RexNode> nodes, RelDataType type);

    /**
     * Executes expression.
     *
     * @param node
     * @param <T>
     * @return
     */
    <T> Supplier<Future<T>> execute(RexNode node);
}
