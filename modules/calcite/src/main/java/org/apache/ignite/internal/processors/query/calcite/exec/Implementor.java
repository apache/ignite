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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.RelSource;
import org.apache.ignite.internal.processors.query.calcite.prepare.RelTarget;
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
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 * Implements a query plan.
 */
public class Implementor implements IgniteRelVisitor<Node<Object[]>>, RelOp<IgniteRel, Node<Object[]>> {
    /** */
    private final ExecutionContext ctx;

    /** */
    private final PartitionService partitionService;

    /** */
    private final ExchangeService exchangeService;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final ScalarFactory scalarFactory;

    /**
     * @param partitionService Affinity service.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeService Exchange service.
     * @param failure Failure processor.
     * @param ctx Root context.
     * @param log Logger.
     */
    public Implementor(PartitionService partitionService, MailboxRegistry mailboxRegistry, ExchangeService exchangeService, FailureProcessor failure, ExecutionContext ctx, IgniteLogger log) {
        this.partitionService = partitionService;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeService = exchangeService;
        this.ctx = ctx;

        scalarFactory = new ScalarFactory(ctx.getTypeFactory(), failure, log);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteSender rel) {
        RelTarget target = rel.target();
        long targetFragmentId = target.fragmentId();
        IgniteDistribution distribution = target.distribution();
        Destination destination = distribution.function().destination(partitionService, target.mapping(), distribution.getKeys());

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<Object[]> outbox = new Outbox<>(exchangeService, mailboxRegistry, ctx, targetFragmentId, ctx.fragmentId(), visit(rel.getInput()), destination);

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteFilter rel) {
        Predicate<Object[]> predicate = scalarFactory.filterPredicate(ctx, rel.getCondition(), rel.getRowType());
        return new FilterNode(ctx, visit(rel.getInput()), predicate);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteProject rel) {
        Function<Object[], Object[]> projection = scalarFactory.projectExpression(ctx, rel.getProjects(), rel.getInput().getRowType());
        return new ProjectNode(ctx, visit(rel.getInput()), projection);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteJoin rel) {
        BiFunction<Object[], Object[], Object[]> expression = scalarFactory.joinExpression(ctx, rel.getCondition(), rel.getLeft().getRowType(), rel.getRight().getRowType());
        return new JoinNode(ctx, visit(rel.getLeft()), visit(rel.getRight()), expression);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteTableScan rel) {
        return new ScanNode(ctx, rel.getTable().unwrap(ScannableTable.class).scan(ctx));
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteReceiver rel) {
        RelSource source = rel.source();

        // Corresponding outbox fragment ID is used as exchange ID as well.
        Inbox<Object[]> inbox = (Inbox<Object[]>) mailboxRegistry.register(new Inbox<>(exchangeService, mailboxRegistry, ctx, source.fragmentId(), source.fragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, source.mapping().nodes(), scalarFactory.comparator(ctx, rel.collations(), rel.getRowType()));

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
        return visit((IgniteRel) rel);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> go(IgniteRel rel) {
        return visit(rel);
    }
}
