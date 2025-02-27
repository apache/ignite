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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A part of exchange.
 */
public class Outbox<Row> extends AbstractNode<Row> implements Mailbox<Row>, SingleNode<Row>, Downstream<Row> {
    /** */
    private final ExchangeService exchange;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long exchangeId;

    /** */
    private final long targetFragmentId;

    /** */
    private final Destination<Row> dest;

    /** */
    private final Deque<Row> inBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private final Map<UUID, Buffer> nodeBuffers = new HashMap<>();

    /** */
    private int waiting;

    /** */
    private boolean exchangeFinished;

    /**
     * @param ctx Execution context.
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param exchangeId Exchange ID.
     * @param targetFragmentId Target fragment ID.
     * @param dest Destination.
     */
    public Outbox(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        ExchangeService exchange,
        MailboxRegistry registry,
        long exchangeId,
        long targetFragmentId,
        Destination<Row> dest
    ) {
        super(ctx, rowType);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.dest = dest;
    }

    /** {@inheritDoc} */
    @Override public long exchangeId() {
        return exchangeId;
    }

    /**
     * callback method.
     *
     * @param nodeId Target ID.
     * @param batchId Batch ID.
     */
    public void onAcknowledge(UUID nodeId, int batchId) throws Exception {
        assert nodeBuffers.containsKey(nodeId);

        checkState();

        nodeBuffers.get(nodeId).acknowledge(batchId);
    }

    /** */
    public void init() {
        try {
            checkState();

            flush();
        }
        catch (Throwable t) {
            onError(t);
        }
    }

    /** {@inheritDoc} */
    @Override public void request(int rowCnt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert waiting > 0;

        checkState();

        waiting--;

        inBuf.add(row);

        flush();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** {@inheritDoc} */
    @Override protected void onErrorInternal(Throwable e) {
        try {
            sendError(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(context().logger(),
                "Error occurred during send error message: " + X.getFullStackTrace(e));
        }
        finally {
            U.closeQuiet(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void closeInternal() {
        super.closeInternal();

        registry.unregister(this);

        // Send cancel message for the Inbox to close Inboxes created by batch message race.
        for (UUID node : dest.targets())
            getOrCreateBuffer(node).close();
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Row> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private void sendBatch(UUID nodeId, int batchId, boolean last, List<Row> rows) throws IgniteCheckedException {
        exchange.sendBatch(nodeId, queryId(), targetFragmentId, exchangeId, batchId, last, rows);
    }

    /** */
    private void sendError(Throwable err) throws IgniteCheckedException {
        exchange.sendError(context().originatingNodeId(), queryId(), fragmentId(), err);
    }

    /** */
    private void sendInboxClose(UUID nodeId) {
        try {
            exchange.closeInbox(nodeId, queryId(), targetFragmentId, exchangeId);
        }
        catch (IgniteCheckedException e) {
            U.warn(context().logger(), "Failed to send cancel message.", e);
        }
    }

    /** */
    private Buffer getOrCreateBuffer(UUID nodeId) {
        return nodeBuffers.computeIfAbsent(nodeId, this::createBuffer);
    }

    /** */
    private Buffer createBuffer(UUID nodeId) {
        return new Buffer(nodeId);
    }

    /** */
    private void flush() throws Exception {
        while (!inBuf.isEmpty()) {
            checkState();

            List<UUID> nodes = dest.targets(inBuf.peek());

            assert !F.isEmpty(nodes);

            List<Buffer> buffers = new ArrayList<>(nodes.size());

            for (UUID id: nodes) {
                Buffer buf = getOrCreateBuffer(id);

                if (!buf.ready())
                    return;

                buffers.add(buf);
            }

            Row row = inBuf.remove();

            for (Buffer dest : buffers)
                dest.add(row);
        }

        assert inBuf.isEmpty();

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
        else if (waiting == -1) {
            for (UUID node : dest.targets())
                getOrCreateBuffer(node).end();

            if (!exchangeFinished) {
                exchange.onOutboundExchangeFinished(queryId(), exchangeId);

                exchangeFinished = true;
            }
        }
    }

    /** */
    public void onNodeLeft(UUID nodeId) {
        if (nodeId.equals(context().originatingNodeId()))
            context().execute(this::close, this::onError);
    }

    /** */
    private final class Buffer {
        /** */
        private final UUID nodeId;

        /** */
        private int hwm = -1;

        /** */
        private int lwm = -1;

        /** */
        private List<Row> curr;

        /** */
        private Buffer(UUID nodeId) {
            this.nodeId = nodeId;

            curr = new ArrayList<>(IO_BATCH_SIZE);
        }

        /**
         * Checks whether there is a place for a new row.
         *
         * @return {@code True} is it possible to add a row to a batch.
         */
        private boolean ready() {
            if (hwm == Integer.MAX_VALUE)
                return false;

            return curr.size() < IO_BATCH_SIZE || hwm - lwm < IO_BATCH_CNT;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(Row row) throws IgniteCheckedException {
            assert ready();

            if (curr.size() == IO_BATCH_SIZE) {
                sendBatch(nodeId, ++hwm, false, curr);

                curr = new ArrayList<>(IO_BATCH_SIZE);
            }

            curr.add(row);
        }

        /**
         * Signals data is over.
         */
        public void end() throws IgniteCheckedException {
            if (hwm == Integer.MAX_VALUE)
                return;

            int batchId = hwm + 1;
            hwm = Integer.MAX_VALUE;

            List<Row> tmp = curr;
            curr = null;

            sendBatch(nodeId, batchId, true, tmp);
        }

        /**
         * Callback method.
         *
         * @param id batch ID.
         */
        private void acknowledge(int id) throws Exception {
            if (lwm > id)
                return;

            boolean readyBefore = ready();

            lwm = id;

            if (!readyBefore && ready())
                flush();
        }

        /** */
        public void close() {
            int currBatchId = hwm;

            if (hwm == Integer.MAX_VALUE)
                return;

            hwm = Integer.MAX_VALUE;

            curr = null;

            if (currBatchId >= 0)
                sendInboxClose(nodeId);
        }
    }
}
