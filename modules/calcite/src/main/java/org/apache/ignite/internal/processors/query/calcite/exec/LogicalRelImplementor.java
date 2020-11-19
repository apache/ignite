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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.CorrelatedNestedLoopJoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.FilterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ModifyNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.NestedLoopJoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ProjectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.UnionAllNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.CollocationGroup;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.util.TypeUtils.combinedRowType;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<Row> implements IgniteRelVisitor<Node<Row>> {
    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    private final AffinityService affSrvc;

    /** */
    private final ExchangeService exchangeSvc;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final ExpressionFactory<Row> expressionFactory;

    /**
     * @param ctx Root context.
     * @param affSrvc Affinity service.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSvc Exchange service.
     * @param failure Failure processor.
     */
    public LogicalRelImplementor(
        ExecutionContext<Row> ctx,
        AffinityService affSrvc,
        MailboxRegistry mailboxRegistry,
        ExchangeService exchangeSvc,
        FailureProcessor failure
    ) {
        this.affSrvc = affSrvc;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSvc = exchangeSvc;
        this.ctx = ctx;

        expressionFactory = ctx.expressionFactory();
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteSender rel) {
        IgniteDistribution distribution = rel.distribution();

        Destination<Row> dest = distribution.destination(ctx, affSrvc, ctx.target());

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<Row> outbox =
            new Outbox<>(ctx, rel.getRowType(), exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.targetFragmentId(), dest);

        Node<Row> input = visit(rel.getInput());

        outbox.register(input);

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteFilter rel) {
        Predicate<Row> pred = expressionFactory.predicate(rel.getCondition(), rel.getRowType());

        FilterNode<Row> node = new FilterNode<>(ctx, rel.getRowType(), pred);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTrimExchange rel) {
        assert TraitUtils.distribution(rel).getType() == HASH_DISTRIBUTED;
        assert TraitUtils.distribution(rel.getInput()).getType() == BROADCAST_DISTRIBUTED;

        IgniteDistribution distr = rel.distribution();
        Destination<Row> dest = distr.destination(ctx, affSrvc, ctx.group(rel.sourceId()));
        UUID localNodeId = ctx.planningContext().localNodeId();

        FilterNode<Row> node = new FilterNode<>(ctx, rel.getRowType(), r -> Objects.equals(localNodeId, F.first(dest.targets(r))));

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteProject rel) {
        Function<Row, Row> prj = expressionFactory.project(rel.getProjects(), rel.getInput().getRowType());

        ProjectNode<Row> node = new ProjectNode<>(ctx, rel.getRowType(), prj);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        Predicate<Row> cond = expressionFactory.predicate(rel.getCondition(), rowType);

        Node<Row> node = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType, cond);

        Node<Row> leftInput = visit(rel.getLeft());
        Node<Row> rightInput = visit(rel.getRight());

        node.register(F.asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        Predicate<Row> cond = expressionFactory.predicate(rel.getCondition(), rowType);

        assert rel.getJoinType() == JoinRelType.INNER; // TODO LEFT, SEMI, ANTI

        Node<Row> node = new CorrelatedNestedLoopJoinNode<>(ctx, outType, cond, rel.getVariablesSet());

        Node<Row> leftInput = visit(rel.getLeft());
        Node<Row> rightInput = visit(rel.getRight());

        node.register(F.asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteIndexScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        ImmutableBitSet requiredColunms = rel.requiredColunms();
        List<RexNode> lowerCond = rel.lowerBound();
        List<RexNode> upperCond = rel.upperBound();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColunms);

        Predicate<Row> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Supplier<Row> lower = lowerCond == null ? null : expressionFactory.rowSource(lowerCond);
        Supplier<Row> upper = upperCond == null ? null : expressionFactory.rowSource(upperCond);
        Function<Row, Row> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        IgniteIndex idx = tbl.getIndex(rel.indexName());

        CollocationGroup group = ctx.group(rel.sourceId());

        Iterable<Row> rowsIter = idx.scan(ctx, group, filters, lower, upper, prj, requiredColunms);

        return new ScanNode<>(ctx, rowType, rowsIter);
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColunms = rel.requiredColunms();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColunms);

        Predicate<Row> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<Row, Row> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        CollocationGroup group = ctx.group(rel.sourceId());

        Iterable<Row> rowsIter = tbl.scan(ctx, group, filters, prj, requiredColunms);

        return new ScanNode<>(ctx, rowType, rowsIter);
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteValues rel) {
        List<RexLiteral> vals = Commons.flat(Commons.cast(rel.getTuples()));

        RelDataType rowType = rel.getRowType();

        return new ScanNode<>(ctx, rowType, expressionFactory.values(vals, rowType));
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteUnionAll rel) {
        UnionAllNode<Row> node = new UnionAllNode<>(ctx, rel.getRowType());

        List<Node<Row>> inputs = Commons.transform(rel.getInputs(), this::visit);

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        SortNode<Row> node = new SortNode<>(ctx, rel.getRowType(), expressionFactory.comparator(collation));

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableModify rel) {
        switch (rel.getOperation()) {
            case INSERT:
            case UPDATE:
            case DELETE:
                ModifyNode<Row> node = new ModifyNode<>(ctx, rel.getRowType(), rel.getTable().unwrap(TableDescriptor.class),
                    rel.getOperation(), rel.getUpdateColumnList());

                Node<Row> input = visit(rel.getInput());

                node.register(input);

                return node;
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteReceiver rel) {
        Inbox<Row> inbox = (Inbox<Row>) mailboxRegistry.register(
            new Inbox<>(ctx, exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.sourceFragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, rel.getRowType(), ctx.remotes(rel.exchangeId()), expressionFactory.comparator(rel.collation()));

        return inbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteAggregate rel) {
        AggregateNode.AggregateType type = AggregateNode.AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.getAggCallList(), inputType);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        AggregateNode<Row> node = new AggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteMapAggregate rel) {
        AggregateNode.AggregateType type = AggregateNode.AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.getAggCallList(), inputType);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        AggregateNode<Row> node = new AggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteReduceAggregate rel) {
        AggregateNode.AggregateType type = AggregateNode.AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.aggregateCalls(), null);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        AggregateNode<Row> node = new AggregateNode<>(ctx, rowType, type, rel.groupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** */
    private Node<Row> visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    /** */
    public <T extends Node<Row>> T go(IgniteRel rel) {
        return (T)visit(rel);
    }
}
