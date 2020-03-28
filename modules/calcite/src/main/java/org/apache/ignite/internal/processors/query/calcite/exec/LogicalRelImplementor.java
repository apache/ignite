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

import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.FilterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.JoinNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ModifyNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ProjectNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.RelTarget;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implements a query plan.
 */
public class LogicalRelImplementor implements IgniteRelVisitor<Node<Object[]>> {
    /** */
    private final ExecutionContext ctx;

    /** */
    private final PartitionService partitionService;

    /** */
    private final ExchangeService exchangeService;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final ExpressionFactory expressionFactory;

    /**
     * @param ctx Root context.
     * @param partitionService Affinity service.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeService Exchange service.
     * @param failure Failure processor.
     */
    public LogicalRelImplementor(ExecutionContext ctx, PartitionService partitionService, MailboxRegistry mailboxRegistry, ExchangeService exchangeService, FailureProcessor failure) {
        this.partitionService = partitionService;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeService = exchangeService;
        this.ctx = ctx;

        final IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        final SqlConformance conformance = ctx.parent().conformance();
        final SqlOperatorTable opTable = ctx.parent().opTable();

        expressionFactory = new ExpressionFactory(typeFactory, conformance, opTable);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteSender rel) {
        RelTarget target = rel.target();
        IgniteDistribution distribution = rel.distribution();
        Destination destination = distribution.function().destination(partitionService, target.mapping(), distribution.getKeys());

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<Object[]> outbox = new Outbox<>(ctx, exchangeService, mailboxRegistry, ctx.fragmentId(), target.fragmentId(), destination);
        outbox.register(visit(rel.getInput()));

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteFilter rel) {
        Predicate<Object[]> predicate = expressionFactory.predicate(ctx, rel.getCondition(), rel.getRowType());
        FilterNode node = new FilterNode(ctx, predicate);
        node.register(visit(rel.getInput()));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteProject rel) {
        Function<Object[], Object[]> projection = expressionFactory.project(ctx, rel.getProjects(), rel.getInput().getRowType());
        ProjectNode node = new ProjectNode(ctx, projection);
        node.register(visit(rel.getInput()));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteJoin rel) {
        RelDataType rowType = Commons.combinedRowType(ctx.getTypeFactory(), rel.getLeft().getRowType(), rel.getRight().getRowType());
        Predicate<Object[]> condition = expressionFactory.predicate(ctx, rel.getCondition(), rowType);
        JoinNode node = new JoinNode(ctx, condition);
        node.register(F.asList(visit(rel.getLeft()), visit(rel.getRight())));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteTableScan rel) {
        return new ScanNode(ctx, rel.getTable().unwrap(ScannableTable.class).scan(ctx));
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteValues rel) {
        return new ScanNode(ctx, expressionFactory.valuesRex(ctx, Commons.flat(Commons.cast(rel.getTuples())), rel.getRowType().getFieldCount()));
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteTableModify rel) {
        switch (rel.getOperation()){
            case INSERT:
            case UPDATE:
            case DELETE:
                ModifyNode node = new ModifyNode(ctx, rel.getTable().unwrap(TableDescriptor.class), rel.getOperation(), rel.getUpdateColumnList());
                node.register(visit(rel.getInput()));

                return node;
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(IgniteReceiver rel) {
        Fragment source = rel.source();

        // Corresponding outbox fragment ID is used as exchange ID as well.
        Inbox<Object[]> inbox = (Inbox<Object[]>) mailboxRegistry.register(new Inbox<>(ctx, exchangeService, mailboxRegistry, source.fragmentId(), source.fragmentId()));

        // here may be an already created (to consume rows from remote nodes) inbox
        // without proper context, we need to init it with a right one.
        inbox.init(ctx, source.mapping().nodes(), expressionFactory.comparator(ctx, rel.collations(), rel.getRowType()));

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

    /** */
    public Node<Object[]> go(IgniteRel rel) {
        return visit(rel);
    }
}
