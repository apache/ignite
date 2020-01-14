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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.message.InboxCancelMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;

/**
 *
 */
public class ExchangeServiceImpl implements ExchangeService, LifecycleAware {
    /** */
    private final IgniteLogger log;

    /** */
    private CalciteQueryProcessor proc;

    /**
     * @param ctx Kernal context.
     */
    public ExchangeServiceImpl(GridKernalContext ctx) {
        log = ctx.log(ExchangeServiceImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void sendBatch(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
        messageService().send(nodeId, new QueryBatchMessage(queryId, fragmentId, exchangeId, batchId, rows));
    }

    /** {@inheritDoc} */
    @Override public void onBatchReceived(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId, List<?> rows) {
        Inbox<?> inbox = mailboxRegistry().inbox(queryId, exchangeId);

        if (inbox == null && batchId == 0)
            // first message sent before a fragment is built
            // note that an inbox source fragment id is also used as an exchange id
            inbox = mailboxRegistry().register(new Inbox<>(baseInboxContext(queryId, fragmentId), exchangeId, exchangeId));

        if (inbox != null)
            inbox.onBatchReceived(nodeId, batchId, rows);
        else if (log.isDebugEnabled()){
             log.debug("Stale batch message received: [" +
                 "caller=" + caller + ", " +
                 "nodeId=" + nodeId + ", " +
                 "queryId=" + queryId + ", " +
                 "fragmentId=" + fragmentId + ", " +
                 "exchangeId=" + exchangeId + ", " +
                 "batchId=" + batchId + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        messageService().send(nodeId, new QueryBatchAcknowledgeMessage(queryId, fragmentId, exchangeId, batchId));
    }

    /** {@inheritDoc} */
    @Override public void onAcknowledge(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        Outbox<?> outbox = mailboxRegistry().outbox(queryId, exchangeId);

        if (outbox != null)
            outbox.onAcknowledge(nodeId, batchId);
        else if (log.isDebugEnabled()) {
            log.debug("Stale acknowledge message received: [" +
                "caller=" + caller + ", " +
                "nodeId=" + nodeId + ", " +
                "queryId=" + queryId + ", " +
                "fragmentId=" + fragmentId + ", " +
                "exchangeId=" + exchangeId + ", " +
                "batchId=" + batchId + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        messageService().send(nodeId, new InboxCancelMessage(queryId, fragmentId, exchangeId, batchId));
    }

    /** {@inheritDoc} */
    @Override public void onCancel(Object caller, UUID nodeId, UUID queryId, long fragmentId, long exchangeId, int batchId) {
        Inbox<?> inbox = mailboxRegistry().inbox(queryId, exchangeId);

        if (inbox != null)
            inbox.cancel();
        else if (log.isDebugEnabled()) {
            log.debug("Stale cancel message received: [" +
                "caller=" + caller + ", " +
                "nodeId=" + nodeId + ", " +
                "queryId=" + queryId + ", " +
                "fragmentId=" + fragmentId + ", " +
                "exchangeId=" + exchangeId + ", " +
                "batchId=" + batchId + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        proc = null;
    }

    /** */
    private QueryTaskExecutor taskExecutor() {
        return proc.taskExecutor();
    }

    /** */
    private MailboxRegistry mailboxRegistry() {
        return proc;
    }

    /** */
    private MessageService messageService() {
        return proc.messageService();
    }

    /**
     * @return Minimal execution context to meet Inbox needs.
     */
    private ExecutionContext baseInboxContext(UUID queryId, long fragmentId) {
        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .logger(log)                        // need to log errors
            .taskExecutor(taskExecutor())       // need to put rows into an inbox buffer
            .mailboxRegistry(mailboxRegistry()) // need to unregister inbox on cancel
            .build();

        return new ExecutionContext(ctx, queryId, fragmentId, null, ImmutableMap.of());
    }
}
