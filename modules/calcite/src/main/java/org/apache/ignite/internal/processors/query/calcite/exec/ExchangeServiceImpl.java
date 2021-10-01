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

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.message.InboxCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.OutboxCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 *
 */
public class ExchangeServiceImpl implements ExchangeService {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ExchangeServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final MessageService msgSrvc;

    public ExchangeServiceImpl(
        QueryTaskExecutor taskExecutor,
        MailboxRegistry mailboxRegistry,
        MessageService msgSrvc
    ) {
        this.taskExecutor = taskExecutor;
        this.mailboxRegistry = mailboxRegistry;
        this.msgSrvc = msgSrvc;

        init();
    }

    /** {@inheritDoc} */
    @Override public <Row> void sendBatch(String nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId,
        boolean last, List<Row> rows) throws IgniteInternalCheckedException {
        msgSrvc.send(
            nodeId,
            FACTORY.queryBatchMessage()
                .queryId(qryId)
                .fragmentId(fragmentId)
                .exchangeId(exchangeId)
                .batchId(batchId)
                .last(last)
                .rows(Commons.cast(rows))
                .build()
        );
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(String nodeId, UUID qryId, long fragmentId, long exchangeId, int batchId)
        throws IgniteInternalCheckedException {
        msgSrvc.send(
            nodeId,
            FACTORY.queryBatchAcknowledgeMessage()
                .queryId(qryId)
                .fragmentId(fragmentId)
                .exchangeId(exchangeId)
                .batchId(batchId)
                .build()
        );
    }

    /** {@inheritDoc} */
    @Override public void closeOutbox(String nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException {
        msgSrvc.send(
            nodeId,
            FACTORY.outboxCloseMessage()
                .queryId(qryId)
                .fragmentId(fragmentId)
                .exchangeId(exchangeId)
                .build()
        );
    }

    /** {@inheritDoc} */
    @Override public void closeInbox(String nodeId, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException {
        msgSrvc.send(
            nodeId,
            FACTORY.inboxCloseMessage()
                .queryId(qryId)
                .fragmentId(fragmentId)
                .exchangeId(exchangeId)
                .build()
        );
    }

    /** {@inheritDoc} */
    @Override public void sendError(String nodeId, UUID qryId, long fragmentId, Throwable err) throws IgniteInternalCheckedException {
        msgSrvc.send(
            nodeId,
            FACTORY.errorMessage()
                .queryId(qryId)
                .fragmentId(fragmentId)
                .error(err)
                .build()
        );
    }

    private void init() {
        msgSrvc.register((n, m) -> onMessage(n, (InboxCloseMessage) m), SqlQueryMessageGroup.INBOX_CLOSE_MESSAGE);
        msgSrvc.register((n, m) -> onMessage(n, (OutboxCloseMessage) m), SqlQueryMessageGroup.OUTBOX_CLOSE_MESSAGE);
        msgSrvc.register((n, m) -> onMessage(n, (QueryBatchAcknowledgeMessage) m), SqlQueryMessageGroup.QUERY_BATCH_ACK);
        msgSrvc.register((n, m) -> onMessage(n, (QueryBatchMessage) m), SqlQueryMessageGroup.QUERY_BATCH_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean alive(String nodeId) {
        return msgSrvc.alive(nodeId);
    }

    /** */
    protected void onMessage(String nodeId, InboxCloseMessage msg) {
        Collection<Inbox<?>> inboxes = mailboxRegistry.inboxes(msg.queryId(), msg.fragmentId(), msg.exchangeId());

        if (!nullOrEmpty(inboxes)) {
            for (Inbox<?> inbox : inboxes)
                inbox.context().execute(inbox::close, inbox::onError);
        }
        else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale inbox cancel message received: [" +
                "nodeId=" + nodeId +
                ", queryId=" + msg.queryId() +
                ", fragmentId=" + msg.fragmentId() +
                ", exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(String nodeId, OutboxCloseMessage msg) {
        Collection<Outbox<?>> outboxes = mailboxRegistry.outboxes(msg.queryId(), msg.fragmentId(), msg.exchangeId());

        if (!nullOrEmpty(outboxes)) {
            for (Outbox<?> outbox : outboxes)
                outbox.context().execute(outbox::close, outbox::onError);

            for (Outbox<?> outbox : outboxes)
                outbox.context().execute(outbox.context()::cancel, outbox::onError);
        }
        else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale outbox cancel message received: [" +
                "nodeId=" + nodeId +
                ", queryId=" + msg.queryId() +
                ", fragmentId=" + msg.fragmentId() +
                ", exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(String nodeId, QueryBatchAcknowledgeMessage msg) {
        Outbox<?> outbox = mailboxRegistry.outbox(msg.queryId(), msg.exchangeId());

        if (outbox != null) {
            try {
                outbox.onAcknowledge(nodeId, msg.batchId());
            }
            catch (Throwable e) {
                outbox.onError(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        }
        else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale acknowledge message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "fragmentId=" + msg.fragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");
        }
    }

    /** */
    protected void onMessage(String nodeId, QueryBatchMessage msg) {
        Inbox<?> inbox = mailboxRegistry.inbox(msg.queryId(), msg.exchangeId());

        if (inbox == null && msg.batchId() == 0) {
            // first message sent before a fragment is built
            // note that an inbox source fragment id is also used as an exchange id
            Inbox<?> newInbox = new Inbox<>(baseInboxContext(nodeId, msg.queryId(), msg.fragmentId()),
                this, mailboxRegistry, msg.exchangeId(), msg.exchangeId());

            inbox = mailboxRegistry.register(newInbox);
        }

        if (inbox != null) {
            try {
                inbox.onBatchReceived(nodeId, msg.batchId(), msg.last(), Commons.cast(msg.rows()));
            }
            catch (Throwable e) {
                inbox.onError(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        }
        else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale batch message received: [" +
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
    private ExecutionContext<?> baseInboxContext(String nodeId, UUID qryId, long fragmentId) {
        return new ExecutionContext<>(
            taskExecutor,
            PlanningContext.builder()
                .originatingNodeId(nodeId)
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
