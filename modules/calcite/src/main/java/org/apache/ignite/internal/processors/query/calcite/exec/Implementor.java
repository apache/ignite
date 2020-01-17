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

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.ScannableTable;
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
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSource;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelTarget;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 * Implements a query plan.
 */
public class Implementor implements IgniteRelVisitor<Node<Object[]>>, RelOp<IgniteRel, Node<Object[]>> {
    /** */
    private final ExecutionContext ctx;

    /** */
    private final ScalarFactory factory;

    /**
     * @param ctx Root context.
     */
    public Implementor(ExecutionContext ctx) {
        this.ctx = ctx;

        factory = new ScalarFactory(ctx);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteSender rel) {
        RelTarget target = rel.target();
        long targetFragmentId = target.fragmentId();
        IgniteDistribution distribution = target.distribution();
        DestinationFunction function = distribution.function().toDestination(ctx.parent(), target.mapping(), distribution.getKeys());
        MailboxRegistry registry = ctx.mailboxRegistry();

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<Object[]> outbox = new Outbox<>(ctx, targetFragmentId, ctx.fragmentId(), visit(rel.getInput()), function);

        registry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteFilter rel) {
        Predicate<Object[]> predicate = factory.filterPredicate(ctx, rel.getCondition(), rel.getRowType());
        return new FilterNode(ctx, visit(rel.getInput()), predicate);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteProject rel) {
        Function<Object[], Object[]> projection = factory.projectExpression(ctx, rel.getProjects(), rel.getInput().getRowType());
        return new ProjectNode(ctx, visit(rel.getInput()), projection);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteJoin rel) {
        BiFunction<Object[], Object[], Object[]> expression = factory.joinExpression(ctx, rel.getCondition(), rel.getLeft().getRowType(), rel.getRight().getRowType());
        return new JoinNode(ctx, visit(rel.getLeft()), visit(rel.getRight()), expression);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteTableScan rel) {
        Iterable<Object[]> source = rel.getTable().unwrap(ScannableTable.class).scan(ctx);
        return new ScanNode(ctx, source);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteReceiver rel) {
        RelSource source = rel.source();
        MailboxRegistry registry = ctx.mailboxRegistry();

        // Corresponding outbox fragment ID is used as exchange ID as well.
        Inbox<Object[]> inbox = (Inbox<Object[]>) registry.register(new Inbox<>(ctx, source.fragmentId(), source.fragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, source.mapping().nodes(), factory.comparator(ctx, rel.collations(), rel.getRowType()));

        return inbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** */
    private Node<Object[]> visit(RelNode rel) {
        return visit(igniteRel(rel));
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> go(IgniteRel rel) {
        return visit(rel);
    }
}
