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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
public class Outbox<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row>, AutoCloseable {
    /** */
    private final ExchangeService exchange;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long exchangeId;

    /** */
    private final long targetFragmentId;

    /** */
    private final Destination dest;

    /** */
    private final Deque<Row> inBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private final Map<UUID, Buffer> nodeBuffers = new HashMap<>();

    /** */
    private int waiting;

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
        ExchangeService exchange,
        MailboxRegistry registry,
        long exchangeId, long
        targetFragmentId,
        Destination dest
    ) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.dest = dest;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return context().queryId();
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * callback method.
     *
     * @param nodeId Target ID.
     * @param batchId Batch ID.
     */
    public void onAcknowledge(UUID nodeId, int batchId) {
        nodeBuffers.get(nodeId).onAcknowledge(batchId);
    }

    /** */
    public void init() {
        checkThread();

        flushFromBuffer();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        assert waiting > 0;

        waiting--;

        inBuf.add(row);

        flushFromBuffer();
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert waiting > 0;

        waiting = -1;

        try {
            for (UUID node : dest.targets())
                getOrCreateBuffer(node).end();

            close();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        U.error(context().planningContext().logger(),
            "Error occurred during execution: " + X.getFullStackTrace(e));

        nodeBuffers.values().forEach(b -> b.onError(e));
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (isClosed())
            return;

        registry.unregister(this);

        // Send cancel message for the Inbox to close Inboxes created by batch message race.
        nodeBuffers.values().forEach(Buffer::close);

        super.close();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowCnt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<Row> downstream) {
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
    private void sendError(UUID nodeId, Throwable err) throws IgniteCheckedException {
        exchange.sendError(nodeId, queryId(), targetFragmentId, exchangeId, err);
    }

    /** */
    private void sendCancel(UUID nodeId, int batchId) {
        try {
            exchange.closeInbox(nodeId, queryId(), targetFragmentId, exchangeId, batchId);
        }
        catch (IgniteCheckedException e) {
            U.warn(context().planningContext().logger(), "Failed to send cancel message.", e);
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
    private void flushFromBuffer() {
        try {
            while (!inBuf.isEmpty()) {
                Row row = inBuf.remove();

                List<UUID> nodes = dest.targets(row);

                assert !F.isEmpty(nodes);

                Collection<Buffer> buffers = new ArrayList<>(nodes.size());

                for (UUID node : nodes) {
                    Buffer dest = getOrCreateBuffer(node);

                    if (dest.ready())
                        buffers.add(dest);
                    else {
                        inBuf.addFirst(row);

                        return;
                    }
                }

                for (Buffer dest : buffers)
                    dest.add(row);
            }

            if (waiting == 0)
                F.first(sources).request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            onError(e);
        }
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

            curr = new ArrayList<>(IO_BATCH_SIZE); // extra space for end marker;
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

                curr = new ArrayList<>(IO_BATCH_SIZE); // extra space for end marker;
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

        /** */
        public void close() {
            if (hwm == Integer.MAX_VALUE)
                return;

            int batchId = hwm + 1;
            hwm = Integer.MAX_VALUE;

            curr = null;
            sendCancel(nodeId, batchId);
        }

        /** */
        public void onError(Throwable e) {
            try {
                sendError(nodeId, e);
            }
            catch (IgniteCheckedException ex) {
                U.error(context().planningContext().logger(),
                    "Error occurred during send error message: " + X.getFullStackTrace(e));
            }
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
         * Callback method.
         *
         * @param id batch ID.
         */
        private void onAcknowledge(int id) {
            if (lwm > id)
                return;

            boolean readyBefore = ready();

            lwm = id;

            if (!readyBefore && ready())
                flushFromBuffer();
        }
    }
}
