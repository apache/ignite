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
import org.apache.ignite.internal.processors.query.calcite.message.InboxCancelMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class ExchangeServiceImpl extends AbstractService implements ExchangeService {
    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MessageService messageService;

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
     * @param messageService Message service.
     */
    public void messageService(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * @return  Message service.
     */
    public MessageService messageService() {
        return messageService;
    }

    /** {@inheritDoc} */
    @Override public void sendBatch(Outbox<?> caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
        try {
            messageService().send(nodeId, new QueryBatchMessage(queryId, fragmentId, exchangeId, batchId, rows));
        }
        catch (IgniteCheckedException e) {
            caller.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(Inbox<?> caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        try {
            messageService().send(nodeId, new QueryBatchAcknowledgeMessage(queryId, fragmentId, exchangeId, batchId));
        }
        catch (IgniteCheckedException e) {
            caller.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(Outbox<?> caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        try {
            messageService().send(nodeId, new InboxCancelMessage(queryId, fragmentId, exchangeId, batchId));
        }
        catch (IgniteCheckedException ignored) {
        }
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        CalciteQueryProcessor proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

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

        if (inbox == null && msg.batchId() == 0)
            // first message sent before a fragment is built
            // note that an inbox source fragment id is also used as an exchange id
            inbox = mailboxRegistry().register(new Inbox<>(this, mailboxRegistry(), baseInboxContext(msg.queryId(), msg.fragmentId()), msg.exchangeId(), msg.exchangeId()));

        if (inbox != null)
            inbox.onBatchReceived(nodeId, msg.batchId(), msg.rows());
        else if (log.isDebugEnabled()){
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
    private ExecutionContext baseInboxContext(UUID queryId, long fragmentId) {
        PlanningContext ctx = PlanningContext.builder().logger(log).build();
        return new ExecutionContext(taskExecutor(), ctx, queryId, fragmentId, null, ImmutableMap.of());
    }
}
