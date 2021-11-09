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

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.util.TypeUtils.combinedRowType;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractSetOpNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.CorrelatedNestedLoopJoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.FilterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.HashAggregateNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.IndexSpoolNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.IntersectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.LimitNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.MergeJoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.MinusNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ModifyNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.NestedLoopJoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ProjectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortAggregateNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.TableScanNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.TableSpoolNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.UnionAllNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<RowT> implements IgniteRelVisitor<Node<RowT>> {
    public static final String CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG =
            "only INNER and LEFT join supported by IgniteCorrelatedNestedLoop";

    private final ExecutionContext<RowT> ctx;

    private final AffinityService affSrvc;

    private final ExchangeService exchangeSvc;

    private final MailboxRegistry mailboxRegistry;

    private final ExpressionFactory<RowT> expressionFactory;

    /**
     * Constructor.
     *
     * @param ctx             Root context.
     * @param affSrvc         Affinity service.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSvc     Exchange service.
     */
    public LogicalRelImplementor(
            ExecutionContext<RowT> ctx,
            AffinityService affSrvc,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSvc
    ) {
        this.affSrvc = affSrvc;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSvc = exchangeSvc;
        this.ctx = ctx;

        expressionFactory = ctx.expressionFactory();
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSender rel) {
        IgniteDistribution distribution = rel.distribution();

        Destination<RowT> dest = distribution.destination(ctx, affSrvc, ctx.target());

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<RowT> outbox =
                new Outbox<>(ctx, rel.getRowType(), exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.targetFragmentId(), dest);

        Node<RowT> input = visit(rel.getInput());

        outbox.register(input);

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteFilter rel) {
        Predicate<RowT> pred = expressionFactory.predicate(rel.getCondition(), rel.getRowType());

        FilterNode<RowT> node = new FilterNode<>(ctx, rel.getRowType(), pred);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTrimExchange rel) {
        assert TraitUtils.distribution(rel).getType() == HASH_DISTRIBUTED;

        IgniteDistribution distr = rel.distribution();
        Destination<RowT> dest = distr.destination(ctx, affSrvc, ctx.group(rel.sourceId()));
        String localNodeId = ctx.planningContext().localNodeId();

        FilterNode<RowT> node = new FilterNode<>(ctx, rel.getRowType(), r -> Objects.equals(localNodeId, first(dest.targets(r))));

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteProject rel) {
        Function<RowT, RowT> prj = expressionFactory.project(rel.getProjects(), rel.getInput().getRowType());

        ProjectNode<RowT> node = new ProjectNode<>(ctx, rel.getRowType(), prj);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        Predicate<RowT> cond = expressionFactory.predicate(rel.getCondition(), rowType);

        Node<RowT> node = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType, cond);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        Predicate<RowT> cond = expressionFactory.predicate(rel.getCondition(), rowType);

        assert rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.LEFT
                : CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

        Node<RowT> node = new CorrelatedNestedLoopJoinNode<>(ctx, outType, cond, rel.getVariablesSet(),
                rel.getJoinType());

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMergeJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        int pairsCnt = rel.analyzeCondition().pairs().size();

        Comparator<RowT> comp = expressionFactory.comparator(
                rel.leftCollation().getFieldCollations().subList(0, pairsCnt),
                rel.rightCollation().getFieldCollations().subList(0, pairsCnt)
        );

        Node<RowT> node = MergeJoinNode.create(ctx, outType, leftType, rightType, joinType, comp);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteIndexScan rel) {
        // TODO: fix this
        //        RexNode condition = rel.condition();
        //        List<RexNode> projects = rel.projects();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        ImmutableBitSet requiredColumns = rel.requiredColumns();
        //        List<RexNode> lowerCond = rel.lowerBound();
        //        List<RexNode> upperCond = rel.upperBound();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        //        Predicate<Row> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        //        Supplier<Row> lower = lowerCond == null ? null : expressionFactory.rowSource(lowerCond);
        //        Supplier<Row> upper = upperCond == null ? null : expressionFactory.rowSource(upperCond);
        //        Function<Row, Row> prj = projects == null ? null : expressionFactory.project(projects, rowType);
        //
        //        IgniteIndex idx = tbl.getIndex(rel.indexName());
        //
        //        ColocationGroup group = ctx.group(rel.sourceId());

        Iterable<RowT> rowsIter = (Iterable<RowT>) List.of(new Object[]{0, 0},
                new Object[]{1, 1}); //idx.scan(ctx, group, filters, lower, upper, prj, requiredColumns);

        return new ScanNode<>(ctx, rowType, rowsIter);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColumns = rel.requiredColumns();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<RowT, RowT> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        ColocationGroup group = ctx.group(rel.sourceId());

        return new TableScanNode<>(
                ctx,
                rowType,
                tbl.descriptor(),
                group.partitions(ctx.planningContext().localNodeId()),
                filters,
                prj,
                requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteValues rel) {
        List<RexLiteral> vals = Commons.flat(Commons.cast(rel.getTuples()));

        RelDataType rowType = rel.getRowType();

        return new ScanNode<>(ctx, rowType, expressionFactory.values(vals, rowType));
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteUnionAll rel) {
        UnionAllNode<RowT> node = new UnionAllNode<>(ctx, rel.getRowType());

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteLimit rel) {
        Supplier<Integer> offset = (rel.offset() == null) ? null : expressionFactory.execute(rel.offset());
        Supplier<Integer> fetch = (rel.fetch() == null) ? null : expressionFactory.execute(rel.fetch());

        LimitNode<RowT> node = new LimitNode<>(ctx, rel.getRowType(), offset, fetch);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        SortNode<RowT> node = new SortNode<>(ctx, rel.getRowType(), expressionFactory.comparator(collation));

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableSpool rel) {
        TableSpoolNode<RowT> node = new TableSpoolNode<>(ctx, rel.getRowType(), rel.readType == Spool.Type.LAZY);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSortedIndexSpool rel) {
        RelCollation collation = rel.collation();

        assert rel.indexCondition() != null : rel;

        List<RexNode> lowerBound = rel.indexCondition().lowerBound();
        List<RexNode> upperBound = rel.indexCondition().upperBound();

        Predicate<RowT> filter = expressionFactory.predicate(rel.condition(), rel.getRowType());
        Supplier<RowT> lower = lowerBound == null ? null : expressionFactory.rowSource(lowerBound);
        Supplier<RowT> upper = upperBound == null ? null : expressionFactory.rowSource(upperBound);

        IndexSpoolNode<RowT> node = IndexSpoolNode.createTreeSpool(
                ctx,
                rel.getRowType(),
                collation,
                expressionFactory.comparator(collation),
                filter,
                lower,
                upper
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteHashIndexSpool rel) {
        Supplier<RowT> searchRow = expressionFactory.rowSource(rel.searchRow());

        IndexSpoolNode<RowT> node = IndexSpoolNode.createHashSpool(
                ctx,
                rel.getRowType(),
                ImmutableBitSet.of(rel.keys()),
                searchRow
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSetOp rel) {
        RelDataType rowType = rel.getRowType();

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);

        AbstractSetOpNode<RowT> node;

        if (rel instanceof Minus) {
            node = new MinusNode<>(ctx, rowType, rel.aggregateType(), rel.all(), rowFactory);
        } else if (rel instanceof Intersect) {
            node = new IntersectNode<>(ctx, rowType, rel.aggregateType(), rel.all(), rowFactory, rel.getInputs().size());
        } else {
            throw new AssertionError();
        }

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableFunctionScan rel) {
        Supplier<Iterable<Object[]>> dataSupplier = expressionFactory.execute(rel.getCall());

        RelDataType rowType = rel.getRowType();

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        return new ScanNode<>(ctx, rowType, new TableFunctionScan<>(dataSupplier, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableModify rel) {
        switch (rel.getOperation()) {
            case INSERT:
            case UPDATE:
            case DELETE:
                ModifyNode<RowT> node = new ModifyNode<>(ctx, rel.getRowType(), rel.getTable().unwrap(TableDescriptor.class),
                        rel.getOperation(), rel.getUpdateColumnList());

                Node<RowT> input = visit(rel.getInput());

                node.register(input);

                return node;
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReceiver rel) {
        Inbox<RowT> inbox = mailboxRegistry.register(
                new Inbox<>(ctx, exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.sourceFragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, rel.getRowType(), ctx.remotes(rel.exchangeId()), expressionFactory.comparator(rel.collation()));

        return inbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSingleHashAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggCallList(), inputType);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapHashAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggCallList(), inputType);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceHashAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type, rel.getAggregateCalls(), null);
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSingleSortAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggCallList(),
                inputType
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                rowType,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                expressionFactory.comparator(rel.collation())
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapSortAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggCallList(),
                inputType
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                rowType,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                expressionFactory.comparator(rel.collation())
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceSortAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<RowT>>> accFactory = expressionFactory.accumulatorsFactory(
                type,
                rel.getAggregateCalls(),
                null
        );

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                rowType,
                type,
                rel.getGroupSet(),
                accFactory,
                rowFactory,
                expressionFactory.comparator(rel.collation())
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    private Node<RowT> visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    public <T extends Node<RowT>> T go(IgniteRel rel) {
        return (T) visit(rel);
    }
}
