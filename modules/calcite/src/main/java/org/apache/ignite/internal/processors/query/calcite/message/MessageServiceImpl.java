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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class MessageServiceImpl extends AbstractService implements MessageService {
    /** */
    private final GridMessageListener msgLsnr;

    /** */
    protected QueryTaskExecutor taskExecutor;

    /** */
    protected FailureProcessor failureProcessor;

    /** */
    private Marshaller marsh;

    /** */
    private EnumMap<MessageType, MessageListener> lsnrs;

    /** */
    public MessageServiceImpl(GridKernalContext ctx) {
        super(ctx);

        msgLsnr = this::onMessage;
    }

    /**
     * @param taskExecutor Task executor.
     */
    public void taskExecutor(QueryTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * @param marsh Marshaller.
     */
    public void marshaller(Marshaller marsh) {
        this.marsh = marsh;
    }

    /**
     * @param failureProcessor Failure processor.
     */
    public void failureProcessor(FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        CalciteQueryProcessor proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        taskExecutor(proc.taskExecutor());
        failureProcessor(proc.failureProcessor());

        @SuppressWarnings("deprecation")
        Marshaller marsh0 = ctx.config().getMarshaller();

        if (marsh0 == null) // Stubbed context doesn't have a marshaller
            marsh0 = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        marshaller(marsh0);

        Optional.ofNullable(ctx.io()).ifPresent(this::registerListener);
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        Optional.ofNullable(ctx.io()).ifPresent(this::unregisterListener);

        lsnrs = null;
    }

    /** {@inheritDoc} */
    @Override public void send(Collection<UUID> nodeIds, CalciteMessage msg) {
        for (UUID nodeId : nodeIds)
            send(nodeId, msg);
    }

    /** {@inheritDoc} */
    @Override public void send(UUID nodeId, CalciteMessage msg) {
        if (ctx.localNodeId().equals(nodeId))
            onMessage(nodeId, msg, true);
        else {
            byte plc = msg instanceof ExecutionContextAware ?
                ((ExecutionContextAware) msg).ioPolicy() : GridIoPolicy.QUERY_POOL;

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
                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void register(MessageListener lsnr, MessageType type) {
        if (lsnrs == null)
            lsnrs = new EnumMap<>(MessageType.class);

        MessageListener old = lsnrs.put(type, lsnr);

        assert old == null : old;
    }

    /** */
    protected void onMessage(UUID nodeId, CalciteMessage msg, boolean async) {
        if (msg instanceof ExecutionContextAware) {
            ExecutionContextAware msg0 = (ExecutionContextAware) msg;
            taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(nodeId, msg));
        }
        else if (async)
            taskExecutor.execute(IgniteUuid.VM_ID, ThreadLocalRandom.current().nextLong(1024), () -> onMessageInternal(nodeId, msg));
        else
            onMessageInternal(nodeId, msg);
    }

    /** */
    protected boolean prepareMarshal(Message msg) {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage) msg).prepareMarshal(marsh);

            return true;
        }
        catch (IgniteCheckedException e) {
            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }

        return false;
    }

    /** */
    protected boolean prepareUnmarshal(Message msg) {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage) msg).prepareUnmarshal(marsh, U.resolveClassLoader(ctx.config()));

            return true;
        }
        catch (IgniteCheckedException e) {
            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }

        return false;
    }

    /** */
    private void onMessage(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof CalciteMessage)
            onMessage(nodeId, (CalciteMessage) msg, false);
    }

    /** */
    private void onMessageInternal(UUID nodeId, CalciteMessage msg) {
        if (!prepareUnmarshal(msg))
            return;

        MessageListener lsnr = Objects.requireNonNull(lsnrs.get(msg.type()));
        lsnr.onMessage(nodeId, msg);
    }

    /** */
    private void registerListener(GridIoManager mgr) {
        mgr.addMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);
    }

    /** */
    private void unregisterListener(GridIoManager mgr) {
        mgr.removeMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);
    }
}
