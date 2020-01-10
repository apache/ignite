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
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class MessageServiceImpl implements MessageService, LifecycleAware {
    /** */
    private static final GridTopic TOPIC = GridTopic.TOPIC_QUERY;

    private IgniteLogger log;

    /** */
    private CalciteQueryProcessor proc;

    /** */
    private GridKernalContext ctx;

    /** */
    private GridMessageListener msgLsnr;

    /** */
    private Marshaller marsh;

    @Override public void onStart(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(MessageServiceImpl.class);

        proc = (CalciteQueryProcessor) ctx.query().getQueryEngine();

        @SuppressWarnings("deprecation")
        Marshaller marsh0 = ctx.config().getMarshaller();

        if (marsh0 == null) // Stubbed context doesn't have a marshaller
            marsh0 = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        marsh = marsh0;

        msgLsnr = (node, msg, plc) -> onMessage(node, msg);

        ctx.io().addMessageListener(TOPIC, msgLsnr);
    }

    @Override public void onStop() {
        ctx.io().removeMessageListener(TOPIC, msgLsnr);

        msgLsnr = null;
        marsh = null;
        log = null;
        proc = null;
        ctx = null;
    }

    @Override public void send(Collection<UUID> nodeIds, Message msg) {
        for (UUID nodeId : nodeIds)
            send(nodeId, msg);
    }

    @Override public void send(UUID nodeId, Message msg) {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage) msg).prepareMarshal(marsh);
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            return;
        }

        try {
            ctx.io().sendToGridTopic(nodeId, TOPIC, msg, GridIoPolicy.QUERY_POOL);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message, node failed: " + nodeId);
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    @Override public void onMessage(UUID nodeId, Object msg) {
        if (msg instanceof MarshalableMessage) {
            try {
                ((MarshalableMessage) msg).prepareUnmarshal(marsh, U.resolveClassLoader(ctx.config()));
            }
            catch (IgniteCheckedException e) {
                ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                return;
            }
        }

        if (msg instanceof QueryStartRequest)
            onQueryStartRequest(nodeId, (QueryStartRequest) msg);
        else if (msg instanceof QueryStartResponse)
            onQueryStartResponse(nodeId, (QueryStartResponse) msg);
        else if (msg instanceof QueryCancelRequest)
            onQueryCancelRequest((QueryCancelRequest) msg);
    }

    /** */
    private void onQueryStartRequest(UUID nodeId, QueryStartRequest req) {
        proc.executeFragment(
            nodeId,
            req.queryId(), req.fragmentId(), req.schema(),
            req.topologyVersion(),
            req.plan(),
            req.partitions(),
            req.parameters());
    }

    private void onQueryStartResponse(UUID nodeId, QueryStartResponse msg) {
        proc.onQueryStarted(nodeId, msg.queryId(), msg.fragmentId(), msg.error());
    }

    private void onQueryCancelRequest(QueryCancelRequest req) {
        proc.cancel(req.queryId());
    }
}
