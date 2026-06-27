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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
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
    private UUID locNodeId;

    /** */
    private final GridIoManager ioMgr;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private FailureProcessor failureProc;

    /** */
    private Map<Class<? extends Message>, MessageListener> lsnrs;

    /** */
    public MessageServiceImpl(GridKernalContext ctx) {
        super(ctx);

        this.ctx = ctx.cache().context();
        ioMgr = ctx.io();
        msgLsnr = this::onMessage;
    }

    /**
     * @param locNodeId Local node ID.
     */
    public void localNodeId(UUID locNodeId) {
        this.locNodeId = locNodeId;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return locNodeId;
    }

    /**
     * @return IO manager.
     */
    public GridIoManager ioManager() {
        return ioMgr;
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
     * @param failureProc Failure processor.
     */
    public void failureProcessor(FailureProcessor failureProc) {
        this.failureProc = failureProc;
    }

    /**
     * @return Failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProc;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        localNodeId(ctx.localNodeId());

        CalciteQueryProcessor proc = queryProcessor(ctx);

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
    @Override public void send(UUID nodeId, Message msg) throws IgniteCheckedException {
        if (localNodeId().equals(nodeId))
            onMessage(nodeId, msg);
        else
            ioManager().sendToGridTopic(nodeId, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.CALLER_THREAD);
    }

    /** {@inheritDoc} */
    @Override public <T extends Message> void register(MessageListener lsnr, Class<T> type) {
        if (lsnrs == null)
            lsnrs = new HashMap<>();

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
    protected void onMessage(UUID nodeId, Message msg) {
        if (msg instanceof ExecutionContextAware) {
            ExecutionContextAware msg0 = (ExecutionContextAware)msg;
            taskExecutor().execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(nodeId, msg));
        }
        else
            taskExecutor().execute(
                IgniteUuid.VM_ID,
                ThreadLocalRandom.current().nextLong(1024),
                () -> onMessageInternal(nodeId, msg)
            );
    }

    /** */
    private void onMessage(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof Message && CalciteMessageFactory.isCalciteMessage((Message)msg))
            onMessage(nodeId, (Message)msg);
    }

    /** */
    private void onMessageInternal(UUID nodeId, Message msg) {
        MessageListener lsnr = Objects.requireNonNull(lsnrs.get(msg.getClass()));

        lsnr.onMessage(nodeId, msg);
    }
}
