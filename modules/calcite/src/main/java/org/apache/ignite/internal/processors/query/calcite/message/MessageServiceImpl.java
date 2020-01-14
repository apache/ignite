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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_ACKNOWLEDGE_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_BATCH_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_CANCEL_REQUEST;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_INBOX_CANCEL_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_START_REQUEST;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_START_RESPONSE;

/**
 *
 */
public class MessageServiceImpl implements MessageService, LifecycleAware {

    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private CalciteQueryProcessor proc;

    /** */
    private GridMessageListener msgLsnr;

    /** */
    private Marshaller marsh;

    public MessageServiceImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(MessageServiceImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        @SuppressWarnings("deprecation")
        Marshaller marsh0 = ctx.config().getMarshaller();

        if (marsh0 == null) // Stubbed context doesn't have a marshaller
            marsh0 = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        marsh = marsh0;

        msgLsnr = (node, msg, plc) -> onMessage(node, msg);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        ctx.io().removeMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);

        msgLsnr = null;
        marsh = null;
        proc = null;
    }

    /** {@inheritDoc} */
    @Override public void send(Collection<UUID> nodeIds, Message msg) {
        for (UUID nodeId : nodeIds)
            send(nodeId, msg);
    }

    /** {@inheritDoc} */
    @Override public void send(UUID nodeId, Message msg) {
        byte plc = msg instanceof ExecutionContextAware ?
            ((ExecutionContextAware) msg).ioPolicy() : GridIoPolicy.QUERY_POOL;

        if (ctx.localNodeId().equals(nodeId)) {
            ctx.closure().runLocalSafe(() -> onMessage(nodeId, msg), plc);

            return;
        }

        if (!prepareMarshal(msg))
            return;

        try {
            ctx.io().sendToGridTopic(nodeId, GridTopic.TOPIC_QUERY, msg, plc);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message, node failed: " + nodeId);
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg) {
        if (!(msg instanceof Message)) {
            log.warning("Unexpected message type:" + msg);

            return;
        }

        Message msg0 = (Message) msg;

        if (!(msg0 instanceof ExecutionContextAware))
            onMessageInternal(nodeId, msg0);
        else {
            ExecutionContextAware contextAware = (ExecutionContextAware) msg0;

            proc.taskExecutor().execute(contextAware.queryId(), contextAware.fragmentId(), () -> onMessageInternal(nodeId, msg0));
        }
    }

    /** */
    private void onMessageInternal(UUID nodeId, Message msg) {
        if (!prepareUnmarshal(msg))
            return;

        switch (msg.directType()) {
            case QUERY_ACKNOWLEDGE_MESSAGE:
                processMessage(nodeId, (QueryBatchAcknowledgeMessage) msg);

                break;
            case QUERY_BATCH_MESSAGE:
                processMessage(nodeId, (QueryBatchMessage) msg);

                break;
            case QUERY_CANCEL_REQUEST:
                processMessage(nodeId, (QueryCancelRequest) msg);

                break;
            case QUERY_INBOX_CANCEL_MESSAGE:
                processMessage(nodeId, (InboxCancelMessage) msg);

                break;
            case QUERY_START_REQUEST:
                processMessage(nodeId, (QueryStartRequest) msg);

                break;
            case QUERY_START_RESPONSE:
                processMessage(nodeId, (QueryStartResponse) msg);

                break;
        }
    }

    /** */
    private boolean prepareMarshal(Message msg) {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage) msg).prepareMarshal(marsh);

            return true;
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }

        return false;
    }

    /** */
    private boolean prepareUnmarshal(Message msg) {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage) msg).prepareUnmarshal(marsh, U.resolveClassLoader(ctx.config()));

            return true;
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }

        return false;
    }

    /** */
    private void processMessage(UUID nodeId, InboxCancelMessage msg) {
        proc.exchangeService().cancel(this, nodeId, msg.queryId(), msg.fragmentId(), msg.exchangeId(), msg.batchId());
    }

    /** */
    private void processMessage(UUID nodeId, QueryCancelRequest msg) {
        proc.cancel(msg.queryId());
    }

    /** */
    private void processMessage(UUID nodeId, QueryBatchMessage msg) {
        proc.exchangeService().onBatchReceived(this, nodeId, msg.queryId(), msg.fragmentId(), msg.exchangeId(), msg.batchId(), msg.rows());
    }

    /** */
    private void processMessage(UUID nodeId, QueryBatchAcknowledgeMessage msg) {
        proc.exchangeService().onAcknowledge(this, nodeId, msg.queryId(), msg.fragmentId(), msg.exchangeId(), msg.batchId());
    }

    /** */
    private void processMessage(UUID nodeId, QueryStartRequest msg) {
        proc.executeFragment(nodeId, msg.queryId(), msg.fragmentId(), msg.schema(), msg.topologyVersion(), msg.plan(), msg.partitions(), msg.parameters());
    }

    /** */
    private void processMessage(UUID nodeId, QueryStartResponse msg) {
        proc.onQueryStarted(nodeId, msg.queryId(), msg.fragmentId(), msg.error());
    }
}
