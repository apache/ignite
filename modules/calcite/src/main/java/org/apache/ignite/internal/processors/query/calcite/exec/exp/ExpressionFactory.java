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
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Expression factory.
 */
public interface ExpressionFactory<Row> {
    /** */
    IgniteTypeFactory typeFactory();

    /** */
    RexBuilder rexBuilder();

    /** */
    Supplier<List<AccumulatorWrapper>> wrappersFactory(ExecutionContext<Row> root,
        AggregateNode.AggregateType type, List<AggregateCall> calls, RelDataType rowType);

    /**
     * Creates a comparator for given data type and collations. Mainly used for sorted exchange.
     *
     * @param collations Collations.
     * @return Row comparator.
     */
    Comparator<Row> comparator(RelCollation collations);

    /**
     * Creates a Filter predicate.
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param filter Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    Predicate<Row> predicate(ExecutionContext<Row> ctx, RexNode filter, RelDataType rowType);

    /**
     * Creates a Filter predicate.
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param filters Filters expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    Predicate<Row> predicate(ExecutionContext<Row> ctx, List<RexNode> filters, RelDataType rowType);

    /**
     * Creates a Project function. Resulting function returns a row with different fields,
     * fields order, fields types, etc.
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param projects Projection expressions.
     * @param rowType Input row type.
     * @return Project function.
     */
    Function<Row, Row> project(ExecutionContext<Row> ctx, List<RexNode> projects, RelDataType rowType);

    /**
     * Creates a Values relational node rows source.
     *
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param values Values.
     * @param rowLen Row length.
     * @return Values relational node rows source.
     */
    Iterable<Row> values(ExecutionContext<Row> ctx, List<RexLiteral> values, int rowLen);

    /**
     * Creates objects row from RexNodes.
     *
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param values Values.
     * @return Values relational node rows source.
     */
    Object[] convertToObjects(ExecutionContext<Row> ctx, List<RexNode> values, RelDataType rowType);

    /**
     * Creates {@link Scalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return Scalar.
     */
    Scalar scalar(RexNode node, RelDataType type);

    /**
     * Creates {@link Scalar}, a code-generated expressions evaluator.
     *
     * @param nodes Expressions.
     * @param type Row type.
     * @return Scalar.
     */
    Scalar scalar(List<RexNode> nodes, RelDataType type);
}
