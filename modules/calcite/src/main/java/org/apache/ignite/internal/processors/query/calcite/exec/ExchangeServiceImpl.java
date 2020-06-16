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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.message.ErrorMessage;
import org.apache.ignite.internal.processors.query.calcite.message.InboxCancelMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
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
    @Override public void cancel(UUID nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId) throws IgniteCheckedException {
        messageService().send(nodeId, new InboxCancelMessage(qryId, fragmentId, exchangeId, batchId));
    }

    /** {@inheritDoc} */
    @Override public void sendError(
        UUID nodeId,
        UUID qryId,
        long fragmentId,
        long exchangeId,
        Throwable err
    ) throws IgniteCheckedException {
        messageService().send(nodeId,
            new ErrorMessage(qryId, fragmentId, exchangeId, messageService().marshaller().marshal(err)));
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
        messageService().register((n, m) -> onMessage(n, (InboxCancelMessage) m), MessageType.QUERY_INBOX_CANCEL_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchAcknowledgeMessage) m), MessageType.QUERY_ACKNOWLEDGE_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchMessage) m), MessageType.QUERY_BATCH_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (ErrorMessage) m), MessageType.QUERY_ERROR_MESSAGE);
    }

    /** */
    protected void onMessage(UUID nodeId, InboxCancelMessage msg) {
        Inbox<?> inbox = mailboxRegistry().inbox(msg.queryId(), msg.exchangeId());

        if (inbox != null)
            inbox.cancel();
        else if (log.isDebugEnabled()) {
            log.debug("Stale cancel message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, ErrorMessage msg) {
        Inbox<?> inbox = mailboxRegistry().inbox(msg.queryId(), msg.exchangeId());

        if (inbox != null) {
            try {
                inbox.onError(
                    nodeId,
                    msg.queryId(),
                    msg.fragmentId(),
                    msg.exchangeId(),
                    messageService().marshaller().unmarshal(msg.errorBytes(), messageService().classLoader()));
            }
            catch (IgniteCheckedException e) {
                log.error("Error on exception unmarshalling", e);
            }
        }
        else if (log.isDebugEnabled()) {
            log.debug("Stale error message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, QueryBatchAcknowledgeMessage msg) {
        Outbox<?> outbox = mailboxRegistry().outbox(msg.queryId(), msg.exchangeId());

        if (outbox != null)
            outbox.onAcknowledge(nodeId, msg.batchId());
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
            Inbox<?> newInbox = new Inbox<>(baseInboxContext(msg.queryId(), msg.fragmentId()),
                this, mailboxRegistry(), msg.exchangeId(), msg.exchangeId());

            inbox = mailboxRegistry().register(newInbox);
        }

        if (inbox != null)
            inbox.onBatchReceived(nodeId, msg.batchId(), msg.last(), Commons.cast(msg.rows()));
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
    private ExecutionContext<?> baseInboxContext(UUID qryId, long fragmentId) {
        return new ExecutionContext<>(
            taskExecutor(),
            PlanningContext.builder()
                .logger(log)
                .build(),
            qryId,
            new FragmentDescription(
                fragmentId,
                null,
                -1,
                null,
                null),
            null,
            ImmutableMap.of());
    }
}
