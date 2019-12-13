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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exchange.Outbox;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunctionFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

import static org.apache.ignite.internal.processors.query.calcite.prepare.ContextValue.PLANNER_CONTEXT;
import static org.apache.ignite.internal.processors.query.calcite.prepare.ContextValue.QUERY_ID;

/**
 *
 */
public class Implementor implements IgniteRelVisitor<Node<Object[]>>, RelOp<IgniteRel, Node<Object[]>> {
    private final PlannerContext ctx;
    private final DataContext root;
    private final ScalarFactory factory;
    private Deque<Sink<Object[]>> stack;

    public Implementor(DataContext root) {
        this.root = root;

        ctx = PLANNER_CONTEXT.get(root);
        factory = new ScalarFactory(new RexBuilder(ctx.typeFactory()));
        stack = new ArrayDeque<>();
    }

    @Override public Node<Object[]> visit(IgniteSender rel) {
        assert stack.isEmpty();

        GridCacheVersion id = QUERY_ID.get(root);
        long exchangeId = rel.target().exchangeId();
        NodesMapping mapping = rel.target().mapping();
        List<UUID> targets = mapping.nodes();
        IgniteDistribution distribution = rel.target().distribution();
        DestinationFunctionFactory destFactory = distribution.destinationFunctionFactory();
        DestinationFunction function = destFactory.create(ctx, mapping, ImmutableIntList.copyOf(distribution.getKeys()));

        Outbox<Object[]> res = new Outbox<>(id, exchangeId, targets, function);

        stack.push(res.sink());

        res.source(source(rel.getInput()));

        return res;
    }

    @Override public Node<Object[]> visit(IgniteFilter rel) {
        assert !stack.isEmpty();

        FilterNode res = new FilterNode(stack.pop(), factory.filterPredicate(root, rel.getCondition(), rel.getRowType()));

        stack.push(res.sink());

        res.source(source(rel.getInput()));

        return res;
    }

    @Override public Node<Object[]> visit(IgniteProject rel) {
        assert !stack.isEmpty();

        ProjectNode res = new ProjectNode(stack.pop(), factory.projectExpression(root, rel.getProjects(), rel.getInput().getRowType()));

        stack.push(res.sink());

        res.source(source(rel.getInput()));

        return res;
    }

    @Override public Node<Object[]> visit(IgniteJoin rel) {
        assert !stack.isEmpty();

        JoinNode res = new JoinNode(stack.pop(), factory.joinExpression(root, rel.getCondition(), rel.getLeft().getRowType(), rel.getRight().getRowType()));

        stack.push(res.sink(1));
        stack.push(res.sink(0));

        res.sources(sources(rel.getInputs()));

        return res;
    }

    @Override public Node<Object[]> visit(IgniteTableScan rel) {
        assert !stack.isEmpty();

        Iterable<Object[]> source = rel.getTable().unwrap(ScannableTable.class).scan(root);

        return new ScanNode(stack.pop(), source);
    }

    @Override public Node<Object[]> visit(IgniteReceiver rel) {
        throw new AssertionError(); // TODO
    }

    @Override public Node<Object[]> visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    @Override public Node<Object[]> visit(IgniteRel other) {
        throw new AssertionError();
    }

    private Source source(RelNode rel) {
        if (rel.getConvention() != IgniteConvention.INSTANCE)
            throw new IllegalStateException("INTERPRETABLE is required.");

        return ((IgniteRel) rel).accept(this);
    }

    private List<Source> sources(List<RelNode> rels) {
        ArrayList<Source> res = new ArrayList<>(rels.size());

        for (RelNode rel : rels) {
            res.add(source(rel));
        }

        return res;
    }

    @Override public Node<Object[]> go(IgniteRel rel) {
        if (rel instanceof IgniteSender)
            return visit((IgniteSender) rel);

        ConsumerNode res = new ConsumerNode();

        stack.push(res.sink());

        res.source(source(rel));

        return res;
    }
}
