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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiPredicate;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractSetOpNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.CollectNode;
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
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanStorageNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortAggregateNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.TableSpoolNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.UncollectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.UnionAllNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCollect;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexBound;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUncollect;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;
import org.apache.ignite.internal.processors.query.calcite.rule.LogicalScanConverterRule;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.sql.SqlKind.IS_DISTINCT_FROM;
import static org.apache.calcite.sql.SqlKind.IS_NOT_DISTINCT_FROM;
import static org.apache.ignite.internal.processors.query.calcite.util.TypeUtils.combinedRowType;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<Row> implements IgniteRelVisitor<Node<Row>> {
    /** */
    public static final String CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG = "only INNER and LEFT join supported by IgniteCorrelatedNestedLoop";

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

        if (distribution.function().affinity()) { // Affinity key can't be null, so filter out null values.
            assert distribution.getKeys().size() == 1 : "Unexpected affinity keys count: " +
                distribution.getKeys().size() + ", must be 1";

            int affKey = distribution.getKeys().get(0);

            RelDataTypeField affFld = rel.getRowType().getFieldList().get(affKey);

            assert affFld != null : "Unexpected affinity key field: " + affKey;

            if (affFld.getType().isNullable()) {
                FilterNode<Row> filter = new FilterNode<>(ctx, rel.getRowType(),
                    r -> ctx.rowHandler().get(affKey, r) != null);

                filter.register(input);

                input = filter;
            }
        }

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

        IgniteDistribution distr = rel.distribution();
        Destination<Row> dest = distr.destination(ctx, affSrvc, ctx.group(rel.sourceId()));
        UUID locNodeId = ctx.localNodeId();

        FilterNode<Row> node = new FilterNode<>(ctx, rel.getRowType(), r -> Objects.equals(locNodeId, F.first(dest.targets(r))));

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

        BiPredicate<Row, Row> cond = expressionFactory.biPredicate(rel.getCondition(), rowType);

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
        BiPredicate<Row, Row> cond = expressionFactory.biPredicate(rel.getCondition(), rowType);

        assert rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.LEFT
            : CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

        Node<Row> node = new CorrelatedNestedLoopJoinNode<>(ctx, outType, cond, rel.getVariablesSet(),
            rel.getJoinType());

        Node<Row> leftInput = visit(rel.getLeft());
        Node<Row> rightInput = visit(rel.getRight());

        node.register(F.asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteMergeJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        int pairsCnt = rel.analyzeCondition().pairs().size();

        Comparator<Row> comp = expressionFactory.comparator(
            rel.leftCollation().getFieldCollations().subList(0, pairsCnt),
            rel.rightCollation().getFieldCollations().subList(0, pairsCnt),
            rel.getCondition().getKind() == IS_NOT_DISTINCT_FROM || rel.getCondition().getKind() == IS_DISTINCT_FROM
        );

        Node<Row> node = MergeJoinNode.create(ctx, outType, leftType, rightType, joinType, comp, hasExchange(rel));

        Node<Row> leftInput = visit(rel.getLeft());
        Node<Row> rightInput = visit(rel.getRight());

        node.register(F.asList(leftInput, rightInput));

        return node;
    }

    /** */
    private boolean hasExchange(RelNode rel) {
        if (rel instanceof IgniteReceiver)
            return true;

        for (RelNode in : rel.getInputs()) {
            if (hasExchange(in))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteIndexScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        ImmutableBitSet requiredColumns = rel.requiredColumns();
        List<SearchBounds> searchBounds = rel.searchBounds();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<Row> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<Row, Row> prj = projects == null ? null : expressionFactory.project(projects, rowType);
        RangeIterable<Row> ranges = searchBounds == null ? null :
            expressionFactory.ranges(searchBounds, rel.collation(), tbl.getRowType(typeFactory));

        ColocationGroup grp = ctx.group(rel.sourceId());

        IgniteIndex idx = tbl.getIndex(rel.indexName());

        if (idx != null && !tbl.isIndexRebuildInProgress()) {
            Iterable<Row> rowsIter = idx.scan(ctx, grp, ranges, requiredColumns);

            return new ScanStorageNode<>(idx.name(), ctx, rowType, rowsIter, filters, prj);
        }
        else {
            // Index was invalidated after planning, workaround through table-scan -> sort -> index spool.
            // If there are correlates in filter or project, spool node is required to provide ability to rewind input.
            // Sort node is required if output should be sorted or if spool node required (to provide search by
            // index conditions).
            // Additionally, project node is required in case of spool inserted, since spool requires unmodified
            // original input for filtering by index conditions.
            boolean filterHasCorrelation = condition != null && RexUtils.hasCorrelation(condition);
            boolean projectHasCorrelation = projects != null && RexUtils.hasCorrelation(projects);
            boolean spoolNodeRequired = projectHasCorrelation || filterHasCorrelation;
            boolean projNodeRequired = projects != null && spoolNodeRequired;

            Iterable<Row> rowsIter = tbl.scan(
                ctx,
                grp,
                requiredColumns
            );

            // If there are projects in the scan node - after the scan we already have target row type.
            if (!spoolNodeRequired && projects != null)
                rowType = rel.getRowType();

            Node<Row> node = new ScanStorageNode<>(tbl.name(), ctx, rowType, rowsIter, filterHasCorrelation ? null : filters,
                projNodeRequired ? null : prj);

            RelCollation collation = rel.collation();

            if ((!spoolNodeRequired && projects != null) || requiredColumns != null) {
                collation = collation.apply(LogicalScanConverterRule.createMapping(
                    spoolNodeRequired ? null : projects,
                    requiredColumns,
                    tbl.getRowType(typeFactory).getFieldCount()
                ));
            }

            boolean sortNodeRequired = !collation.getFieldCollations().isEmpty();

            if (sortNodeRequired) {
                SortNode<Row> sortNode = new SortNode<>(ctx, rowType, expressionFactory.comparator(collation));

                sortNode.register(node);

                node = sortNode;
            }

            if (spoolNodeRequired) {
                if (searchBounds != null && requiredColumns != null) {
                    // Remap index find predicate according to rowType of the spool.
                    List<SearchBounds> remappedSearchBounds = new ArrayList<>(requiredColumns.cardinality());

                    for (int i = requiredColumns.nextSetBit(0); i != -1; i = requiredColumns.nextSetBit(i + 1))
                        remappedSearchBounds.add(searchBounds.get(i));

                    // Collation and row type are already remapped taking into account requiredColumns.
                    ranges = expressionFactory.ranges(remappedSearchBounds, collation, rowType);
                }

                IndexSpoolNode<Row> spoolNode = IndexSpoolNode.createTreeSpool(
                    ctx,
                    rowType,
                    collation,
                    expressionFactory.comparator(collation),
                    filterHasCorrelation ? filters : null, // Not correlated filter included into table scan.
                    ranges
                );

                spoolNode.register(node);

                node = spoolNode;
            }

            if (projNodeRequired) {
                ProjectNode<Row> projectNode = new ProjectNode<>(ctx, rel.getRowType(), prj);

                projectNode.register(node);

                node = projectNode;
            }

            return node;
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteIndexCount rel) {
        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteIndex idx = tbl.getIndex(rel.indexName());

        if (idx != null && !tbl.isIndexRebuildInProgress()) {
            return new ScanStorageNode<>(idx.name() + "_COUNT", ctx, rel.getRowType(),
                idx.count(ctx, ctx.group(rel.sourceId()), rel.notNull()));
        }
        else {
            CollectNode<Row> replacement = CollectNode.createCountCollector(ctx);

            replacement.register(
                new ScanStorageNode<>(
                    tbl.name(),
                    ctx,
                    rel.getTable().getRowType(),
                    tbl.scan(ctx, ctx.group(rel.sourceId()), ImmutableBitSet.of(rel.fieldIndex())),
                    rel.notNull() ? r -> ctx.rowHandler().get(0, r) != null : null,
                    null
                )
            );

            return replacement;
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteIndexBound idxBndRel) {
        IgniteTable tbl = idxBndRel.getTable().unwrap(IgniteTable.class);
        IgniteIndex idx = tbl.getIndex(idxBndRel.indexName());
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        ColocationGroup grp = ctx.group(idxBndRel.sourceId());
        ImmutableBitSet requiredColumns = idxBndRel.requiredColumns();
        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        if (idx != null && !tbl.isIndexRebuildInProgress()) {
            return new ScanStorageNode<>(idx.name() + "_BOUND", ctx, rowType,
                idx.firstOrLast(idxBndRel.first(), ctx, grp, requiredColumns));
        }
        else {
            assert requiredColumns.cardinality() == 1;

            Iterable<Row> rowsIter = tbl.scan(ctx, grp, idxBndRel.requiredColumns());

            Node<Row> scanNode = new ScanStorageNode<>(tbl.name(), ctx, rowType, rowsIter,
                r -> ctx.rowHandler().get(0, r) != null, null);

            RelCollation collation = idx.collation().apply(LogicalScanConverterRule.createMapping(
                null,
                requiredColumns,
                tbl.getRowType(typeFactory).getFieldCount()
            ));

            Comparator<Row> cmp = expressionFactory.comparator(collation);

            assert cmp != null;

            SortNode<Row> sortNode = new SortNode<>(
                ctx,
                rowType,
                idxBndRel.first() ? cmp : cmp.reversed(),
                null,
                () -> 1
            );

            sortNode.register(scanNode);

            return sortNode;
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColumns = rel.requiredColumns();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<Row> filters = condition == null ? null : expressionFactory.predicate(condition, rowType);
        Function<Row, Row> prj = projects == null ? null : expressionFactory.project(projects, rowType);

        ColocationGroup grp = ctx.group(rel.sourceId());

        IgniteIndex idx = tbl.getIndex(QueryUtils.PRIMARY_KEY_INDEX);

        if (idx != null && !tbl.isIndexRebuildInProgress()) {
            Iterable<Row> rowsIter = idx.scan(ctx, grp, null, requiredColumns);

            return new ScanStorageNode<>(idx.name(), ctx, rowType, rowsIter, filters, prj);
        }
        else {
            Iterable<Row> rowsIter = tbl.scan(ctx, grp, requiredColumns);

            return new ScanStorageNode<>(tbl.name(), ctx, rowType, rowsIter, filters, prj);
        }
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
    @Override public Node<Row> visit(IgniteLimit rel) {
        Supplier<Integer> offset = (rel.offset() == null) ? null : expressionFactory.execute(rel.offset());
        Supplier<Integer> fetch = (rel.fetch() == null) ? null : expressionFactory.execute(rel.fetch());

        LimitNode<Row> node = new LimitNode<>(ctx, rel.getRowType(), offset, fetch);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        Supplier<Integer> offset = (rel.offset == null) ? null : expressionFactory.execute(rel.offset);
        Supplier<Integer> fetch = (rel.fetch == null) ? null : expressionFactory.execute(rel.fetch);

        SortNode<Row> node = new SortNode<>(ctx, rel.getRowType(), expressionFactory.comparator(collation), offset,
            fetch);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableSpool rel) {
        TableSpoolNode<Row> node = new TableSpoolNode<>(ctx, rel.getRowType(), rel.readType == Spool.Type.LAZY);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteSortedIndexSpool rel) {
        RelCollation collation = rel.collation();

        assert rel.searchBounds() != null : rel;

        Predicate<Row> filter = expressionFactory.predicate(rel.condition(), rel.getRowType());
        RangeIterable<Row> ranges = expressionFactory.ranges(rel.searchBounds(), collation, rel.getRowType());

        IndexSpoolNode<Row> node = IndexSpoolNode.createTreeSpool(
            ctx,
            rel.getRowType(),
            collation,
            expressionFactory.comparator(collation),
            filter,
            ranges
        );

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteHashIndexSpool rel) {
        Supplier<Row> searchRow = expressionFactory.rowSource(rel.searchRow());

        Predicate<Row> filter = expressionFactory.predicate(rel.condition(), rel.getRowType());

        IndexSpoolNode<Row> node = IndexSpoolNode.createHashSpool(
            ctx,
            rel.getRowType(),
            ImmutableBitSet.of(rel.keys()),
            filter,
            searchRow,
            rel.allowNulls()
        );

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteSetOp rel) {
        RelDataType rowType = rel.getRowType();

        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        List<Node<Row>> inputs = Commons.transform(rel.getInputs(), this::visit);

        AbstractSetOpNode<Row> node;

        if (rel instanceof Minus)
            node = new MinusNode<>(ctx, rowType, rel.aggregateType(), rel.all(), rowFactory);
        else if (rel instanceof Intersect)
            node = new IntersectNode<>(ctx, rowType, rel.aggregateType(), rel.all(), rowFactory, rel.getInputs().size());
        else
            throw new AssertionError();

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableFunctionScan rel) {
        Supplier<Iterable<?>> dataSupplier = expressionFactory.execute(rel.getCall());

        RelDataType rowType = rel.getRowType();

        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        return new ScanNode<>(ctx, rowType, new TableFunctionScan<>(rowType, dataSupplier, rowFactory));
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteTableModify rel) {
        switch (rel.getOperation()) {
            case INSERT:
            case UPDATE:
            case DELETE:
            case MERGE:
                ModifyNode<Row> node = new ModifyNode<>(ctx, rel.getRowType(), rel.getTable().unwrap(CacheTableDescriptor.class),
                    rel.getOperation(), rel.getUpdateColumnList());

                Node<Row> input = visit(rel.getInput());

                node.register(input);

                return node;
            default:
                throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteReceiver rel) {
        Inbox<Row> inbox = (Inbox<Row>)mailboxRegistry.register(
            new Inbox<>(ctx, exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.sourceFragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, rel.getRowType(), ctx.remotes(rel.exchangeId()), expressionFactory.comparator(rel.collation()));

        return inbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteColocatedHashAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.getAggCallList(), inputType);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<Row> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteMapHashAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.getAggCallList(), inputType);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<Row> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteReduceHashAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type, rel.getAggregateCalls(), null);
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        HashAggregateNode<Row> node = new HashAggregateNode<>(ctx, rowType, type, rel.getGroupSets(), accFactory, rowFactory);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteColocatedSortAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type,
            rel.getAggCallList(),
            inputType
        );

        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<Row> node = new SortAggregateNode<>(
            ctx,
            rowType,
            type,
            rel.getGroupSet(),
            accFactory,
            rowFactory,
            expressionFactory.comparator(rel.collation())
        );

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteMapSortAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type,
            rel.getAggCallList(),
            inputType
        );

        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<Row> node = new SortAggregateNode<>(
            ctx,
            rowType,
            type,
            rel.getGroupSet(),
            accFactory,
            rowFactory,
            expressionFactory.comparator(rel.collation())
        );

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteReduceSortAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        Supplier<List<AccumulatorWrapper<Row>>> accFactory = expressionFactory.accumulatorsFactory(
            type,
            rel.getAggregateCalls(),
            null
        );

        RowFactory<Row> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        SortAggregateNode<Row> node = new SortAggregateNode<>(
            ctx,
            rowType,
            type,
            rel.getGroupSet(),
            accFactory,
            rowFactory,
            expressionFactory.comparator(rel.collation())
            );

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteCollect rel) {
        RelDataType outType = rel.getRowType();

        CollectNode<Row> node = new CollectNode<>(ctx, outType);

        Node<Row> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Row> visit(IgniteUncollect rel) {
        UncollectNode<Row> node = new UncollectNode<>(
            ctx,
            rel.getInput().getRowType(),
            rel.getRowType(),
            rel.withOrdinality
        );

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
        return visit((IgniteRel)rel);
    }

    /** */
    public <T extends Node<Row>> T go(IgniteRel rel) {
        return (T)visit(rel);
    }
}
