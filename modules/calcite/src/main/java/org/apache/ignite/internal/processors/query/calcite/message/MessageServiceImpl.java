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

import java.util.EnumMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class MessageServiceImpl extends AbstractService implements MessageService {
    /** */
    private final GridMessageListener msgLsnr;

    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private UUID localNodeId;

    /** */
    private final GridIoManager ioManager;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProcessor;

    /** */
    private EnumMap<MessageType, MessageListener> lsnrs;

    /** */
    public MessageServiceImpl(GridKernalContext ctx) {
        super(ctx);

        this.ctx = ctx.cache().context();
        this.ioManager = ctx.io();
        msgLsnr = this::onMessage;
    }

    /**
     * @param localNodeId Local node ID.
     */
    public void localNodeId(UUID localNodeId) {
        this.localNodeId = localNodeId;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return localNodeId;
    }

    /**
     * @return IO manager.
     */
    public GridIoManager ioManager() {
        return ioManager;
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
     * @param failureProcessor Failure processor.
     */
    public void failureProcessor(FailureProcessor failureProcessor) {
        this.failureProcessor = failureProcessor;
    }

    /**
     * @return Failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        localNodeId(ctx.localNodeId());

        CalciteQueryProcessor proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        taskExecutor(proc.taskExecutor());
        failureProcessor(proc.failureProcessor());

        init();
    }

    /** {@inheritDoc} */
    @Override public void init() {
        ioManager().addMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        ioManager().removeMessageListener(GridTopic.TOPIC_QUERY, msgLsnr);
        lsnrs = null;
    }

    /** {@inheritDoc} */
    @Override public void send(UUID nodeId, CalciteMessage msg) throws IgniteCheckedException {
        if (localNodeId().equals(nodeId))
            onMessage(nodeId, msg);
        else {
            prepareMarshal(msg);

            ioManager().sendToGridTopic(nodeId, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.CALLER_THREAD);
        }
    }

    /** {@inheritDoc} */
    @Override public void register(MessageListener lsnr, MessageType type) {
        if (lsnrs == null)
            lsnrs = new EnumMap<>(MessageType.class);

        MessageListener old = lsnrs.put(type, lsnr);

        assert old == null : old;
    }

    /** {@inheritDoc} */
    @Override public boolean alive(UUID nodeId) {
        try {
            return !ioManager().checkNodeLeft(nodeId, null, false);
        }
        catch (IgniteClientDisconnectedCheckedException e) {
            throw new AssertionError(e);
        }
    }

    /** */
    protected void prepareMarshal(Message msg) throws IgniteCheckedException {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage)msg).prepareMarshal(ctx);
        }
        catch (Exception e) {
            failureProcessor().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** */
    protected void prepareUnmarshal(Message msg) throws IgniteCheckedException {
        try {
            if (msg instanceof MarshalableMessage)
                ((MarshalableMessage)msg).prepareUnmarshal(ctx);
        }
        catch (Exception e) {
            failureProcessor().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** */
    protected void onMessage(UUID nodeId, CalciteMessage msg) {
        if (msg instanceof ExecutionContextAware) {
            ExecutionContextAware msg0 = (ExecutionContextAware)msg;
            taskExecutor().execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(nodeId, msg), msg0.clientContext());
        }
        else
            taskExecutor().execute(
                IgniteUuid.VM_ID,
                ThreadLocalRandom.current().nextLong(1024),
                () -> onMessageInternal(nodeId, msg),
                null
            );
    }

    /** */
    private void onMessage(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof CalciteMessage) {
            try {
                prepareUnmarshal((Message)msg);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            onMessage(nodeId, (CalciteMessage)msg);
        }
    }

    /** */
    private void onMessageInternal(UUID nodeId, CalciteMessage msg) {
        MessageListener lsnr = Objects.requireNonNull(lsnrs.get(msg.type()));
        lsnr.onMessage(nodeId, msg);
    }
}
