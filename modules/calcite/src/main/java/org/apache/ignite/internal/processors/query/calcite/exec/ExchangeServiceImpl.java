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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.message.ErrorMessage;
import org.apache.ignite.internal.processors.query.calcite.message.InboxCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.OutboxCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ExchangeServiceImpl extends AbstractService implements ExchangeService {
    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MessageService msgSvc;

    /**
     * @param ctx Kernal context.
     */
    public ExchangeServiceImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param taskExecutor Task executor.
     */
    public void taskExecutor(QueryTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * @return Task executor.
     */
    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @param mailboxRegistry Mailbox registry.
     */
    public void mailboxRegistry(MailboxRegistry mailboxRegistry) {
        this.mailboxRegistry = mailboxRegistry;
    }

    /**
     * @return  Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @param msgSvc Message service.
     */
    public void messageService(MessageService msgSvc) {
        this.msgSvc = msgSvc;
    }

    /**
     * @return  Message service.
     */
    public MessageService messageService() {
        return msgSvc;
    }

    /** {@inheritDoc} */
    @Override public <Row> void sendBatch(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId,
        boolean last, List<Row> rows) throws IgniteCheckedException {
        messageService().send(nodeId, new QueryBatchMessage(qryId, fragmentId, exchangeId, batchId, last, Commons.cast(rows)));
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId) throws IgniteCheckedException {
        messageService().send(nodeId, new QueryBatchAcknowledgeMessage(qryId, fragmentId, exchangeId, batchId));
    }

    /** {@inheritDoc} */
    @Override public void closeOutbox(UUID nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteCheckedException {
        if (messageService().localNode().equals(nodeId))
            onMessage(nodeId, new OutboxCloseMessage(qryId, fragmentId, exchangeId));
        else
            messageService().send(nodeId, new OutboxCloseMessage(qryId, fragmentId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public void closeInbox(UUID nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteCheckedException {
        if (messageService().localNode().equals(nodeId))
            onMessage(nodeId, new InboxCloseMessage(qryId, fragmentId, exchangeId));
        else
            messageService().send(nodeId, new InboxCloseMessage(qryId, fragmentId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public void sendError(UUID nodeId, UUID qryId, long fragmentId, Throwable err) throws IgniteCheckedException {
        messageService().send(nodeId, new ErrorMessage(qryId, fragmentId, err));
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        CalciteQueryProcessor proc =
            Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        taskExecutor(proc.taskExecutor());
        mailboxRegistry(proc.mailboxRegistry());
        messageService(proc.messageService());

        init();
    }

    /** {@inheritDoc} */
    @Override public void init() {
        messageService().register((n, m) -> onMessage(n, (InboxCloseMessage) m), MessageType.QUERY_INBOX_CANCEL_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (OutboxCloseMessage) m), MessageType.QUERY_OUTBOX_CANCEL_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchAcknowledgeMessage) m), MessageType.QUERY_ACKNOWLEDGE_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchMessage) m), MessageType.QUERY_BATCH_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean alive(UUID nodeId) {
        return messageService().alive(nodeId);
    }

    /** */
    protected void onMessage(UUID nodeId, InboxCloseMessage msg) {
        Collection<Inbox<?>> inboxes = mailboxRegistry().inboxes(msg.queryId(), msg.fragmentId(), msg.exchangeId());

        List<Future<?>> futs = new ArrayList<>(inboxes.size());

        Set<ExecutionContext<?>> ctxs = new HashSet<>();

        for (Inbox<?> inbox : inboxes) {
            Future<?> fut = inbox.context().submit(inbox::close, inbox::onError);

            futs.add(fut);

            ctxs.add(inbox.context());
        }

        for (Future<?> fut : futs) {
            try {
                fut.get();
            }
            catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();

                U.warn(log, e);
            }
        }

        if (log.isDebugEnabled() && F.isEmpty(inboxes)) {
            log.debug("Stale inbox cancel message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, OutboxCloseMessage msg) {
        Collection<Outbox<?>> outboxes = mailboxRegistry().outboxes(msg.queryId(), msg.fragmentId(), msg.exchangeId());

        List<Future<?>> futs = new ArrayList<>(outboxes.size());

        Set<ExecutionContext<?>> ctxs = new HashSet<>();

        for (Outbox<?> outbox : outboxes) {
            Future<?> fut = outbox.context().submit(outbox::close, outbox::onError);

            futs.add(fut);

            ctxs.add(outbox.context());
        }

        for (Future<?> fut : futs) {
            try {
                fut.get();
            }
            catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();

                U.warn(log, e);
            }
        }

        ctxs.forEach(ExecutionContext::cancel);

        if (log.isDebugEnabled() && F.isEmpty(outboxes)) {
            log.debug("Stale oubox cancel message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, QueryBatchAcknowledgeMessage msg) {
        Outbox<?> outbox = mailboxRegistry().outbox(msg.queryId(), msg.exchangeId());

        if (outbox != null) {
            try {
                outbox.onAcknowledge(nodeId, msg.batchId());
            }
            catch (Throwable e) {
                outbox.onError(e);

                throw new IgniteException("Unexpected exception", e);
            }
        }
        else if (log.isDebugEnabled()) {
            log.debug("Stale acknowledge message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, QueryBatchMessage msg) {
        Inbox<?> inbox = mailboxRegistry().inbox(msg.queryId(), msg.exchangeId());

        if (inbox == null && msg.batchId() == 0) {
            // first message sent before a fragment is built
            // note that an inbox source fragment id is also used as an exchange id
            Inbox<?> newInbox = new Inbox<>(baseInboxContext(nodeId, msg.queryId(), msg.fragmentId()),
                this, mailboxRegistry(), msg.exchangeId(), msg.exchangeId());

            inbox = mailboxRegistry().register(newInbox);
        }

        if (inbox != null) {
            try {
                inbox.onBatchReceived(nodeId, msg.batchId(), msg.last(), Commons.cast(msg.rows()));
            }
            catch (Throwable e) {
                inbox.onError(e);

                throw new IgniteException("Unexpected exception", e);
            }
        }
        else if (log.isDebugEnabled()) {
            log.debug("Stale batch message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");
        }
    }

    /**
     * @return Minimal execution context to meet Inbox needs.
     */
    private ExecutionContext<?> baseInboxContext(UUID nodeId, UUID qryId, long fragmentId) {
        return new ExecutionContext<>(
            taskExecutor(),
            PlanningContext.builder()
                .originatingNodeId(nodeId)
                .logger(log)
                .build(),
            qryId,
            new FragmentDescription(
                fragmentId,
                null,
                null,
                null),
            null,
            ImmutableMap.of());
    }
}
