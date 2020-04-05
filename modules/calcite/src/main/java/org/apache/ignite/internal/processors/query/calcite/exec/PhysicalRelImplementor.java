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
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.serialize.FilterPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.JoinPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.PhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.PhysicalRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.serialize.ProjectPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.ReceiverPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.SenderPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.TableModifyPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.TableScanPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.ValuesPhysicalRel;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Converts RelGraph to logical or physical tree.
 */
public class PhysicalRelImplementor implements PhysicalRelVisitor<Node<Object[]>> {
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

    /** */
    public PhysicalRelImplementor(ExecutionContext ctx, PartitionService partitionService, MailboxRegistry mailboxRegistry, ExchangeService exchangeService, FailureProcessor failure) {
        this.ctx = ctx;
        this.partitionService = partitionService;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeService = exchangeService;
        this.expressionFactory = ctx.planningContext().expressionFactory();
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(TableScanPhysicalRel rel) {
        IgniteTable tbl = ctx.planningContext()
            .catalogReader()
            .getTable(rel.tableName())
            .unwrap(IgniteTable.class);

        Predicate<Object[]> filters = null;
        if (rel.condition() != null)
            filters = expressionFactory.predicate(ctx, rel.condition(), rel.rowType());

        Function<Object[], Object[]> proj = null;
        if (rel.projects() != null)
            proj = expressionFactory.project(ctx, rel.projects(), rel.rowType());

        Iterable<Object[]> rowsIter = tbl.scan(ctx, filters, proj);

        return new ScanNode(ctx, rowsIter);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(FilterPhysicalRel rel) {
        FilterNode node = new FilterNode(ctx, expressionFactory.predicate(ctx, rel.condition(), rel.rowType()));
        node.register(visit(rel.input()));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(ProjectPhysicalRel rel) {
        ProjectNode node = new ProjectNode(ctx, expressionFactory.project(ctx, rel.projects(), rel.rowType()));
        node.register(visit(rel.input()));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(JoinPhysicalRel rel) {
        JoinNode node = new JoinNode(ctx, expressionFactory.predicate(ctx, rel.condition(), rel.rowType()));
        node.register(F.asList(visit(rel.left()), visit(rel.right())));

        return node;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(SenderPhysicalRel rel) {
        Destination destination = rel.distributionFunction()
            .destination(partitionService, rel.mapping(), rel.distributionKeys());

        Outbox<Object[]> outbox = new Outbox<>(ctx, exchangeService, mailboxRegistry,
            ctx.fragmentId(), rel.targetFragmentId(), destination);
        outbox.register(visit(rel.input()));

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(ReceiverPhysicalRel rel) {
        Inbox<?> inbox = mailboxRegistry.register(new Inbox<>(ctx, exchangeService, mailboxRegistry, rel.sourceFragmentId(), rel.sourceFragmentId()));

        inbox.init(ctx, rel.sources(), expressionFactory.comparator(ctx, rel.collations(), rel.rowType()));

        return (Node<Object[]>) inbox;
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(ValuesPhysicalRel rel) {
        Iterable<Object[]> source = expressionFactory.valuesExp(ctx, rel.values(), rel.rowLength());

        return new ScanNode(ctx, source);
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(TableModifyPhysicalRel rel) {
        switch (rel.operation()){
            case INSERT:
            case UPDATE:
            case DELETE:
                TableDescriptor desc = ctx.planningContext().catalogReader().getTable(rel.tableName()).unwrap(TableDescriptor.class);
                ModifyNode node = new ModifyNode(ctx, desc, rel.operation(), rel.updateColumnList());
                node.register(visit(rel.input()));

                return node;
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    /** {@inheritDoc} */
    @Override public Node<Object[]> visit(PhysicalRel rel) {
        return rel.accept(this);
    }

    /** */
    public Node<Object[]> go(PhysicalRel root) {
        return visit(root);
    }
}
