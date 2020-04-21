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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.EndMarker;
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
public class Outbox<T> extends AbstractNode<T> implements SingleNode<T>, Downstream<T>, AutoCloseable {
    /** */
    private final ExchangeService exchange;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long exchangeId;

    /** */
    private final long targetFragmentId;

    /** */
    private final Destination destination;

    /** */
    private final Deque<T> inBuffer = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private final Map<UUID, Buffer> nodeBuffers = new HashMap<>();

    /** */
    private boolean cancelled;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param exchangeId Exchange ID.
     * @param targetFragmentId Target fragment ID.
     * @param destination Destination.
     */
    public Outbox(ExecutionContext ctx, ExchangeService exchange, MailboxRegistry registry, long exchangeId, long targetFragmentId, Destination destination) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.destination = destination;
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
    @Override public void push(T row) {
        checkThread();

        assert waiting > 0;

        waiting--;

        inBuffer.add(row);

        flushFromBuffer();
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert waiting > 0;

        waiting = -1;

        try {
            for (UUID node : destination.targets())
                getOrCreateBuffer(node).end();

            close();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        System.out.println("ERROR!=" + X.getFullStackTrace(e));
        cancel(); // TODO send cause to originator.
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();

        context().markCancelled();

        if (cancelled)
            return;

        cancelled = true;

        nodeBuffers.values().forEach(Buffer::cancel);

        close();

        super.cancel();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        registry.unregister(this);
    }

    /** {@inheritDoc} */
    @Override public void request(int rowCount) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onRegister(Downstream<T> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<T> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private void sendBatch(UUID nodeId, int batchId, List<?> rows) throws IgniteCheckedException {
        exchange.sendBatch(nodeId, queryId(), targetFragmentId, exchangeId, batchId, rows);
    }

    /** */
    private void sendCancel(UUID nodeId, int batchId) {
        try {
            exchange.cancel(nodeId, queryId(), targetFragmentId, exchangeId, batchId);
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
        return new Buffer(nodeId, this);
    }

    /** */
    private void flushFromBuffer() {
        try {
            while (!inBuffer.isEmpty()) {
                T row = inBuffer.remove();

                List<UUID> nodes = destination.targets(row);

                assert !F.isEmpty(nodes);

                List<Buffer> buffers = new ArrayList<>(nodes.size());

                for (UUID node : nodes) {
                    Buffer dest = getOrCreateBuffer(node);

                    if (dest.ready())
                        buffers.add(dest);
                    else {
                        inBuffer.addFirst(row);

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
    private static final class Buffer {
        /** */
        private final Outbox<?> owner;

        /** */
        private final UUID nodeId;

        /** */
        private int hwm = -1;

        /** */
        private int lwm = -1;

        /** */
        private List<Object> curr;

        /** */
        private Buffer(UUID nodeId, Outbox<?> owner) {
            this.nodeId = nodeId;
            this.owner = owner;

            curr = new ArrayList<>(IO_BATCH_SIZE + 1); // extra space for end marker;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(Object row) throws IgniteCheckedException {
            assert ready();

            if (curr.size() == IO_BATCH_SIZE) {
                owner.sendBatch(nodeId, ++hwm, curr);

                curr = new ArrayList<>(IO_BATCH_SIZE + 1); // extra space for end marker;
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

            List<Object> tmp = curr;
            curr = null;

            tmp.add(EndMarker.INSTANCE);
            owner.sendBatch(nodeId, batchId, tmp);
        }

        public void cancel() {
            if (hwm == Integer.MAX_VALUE)
                return;

            int batchId = hwm + 1;
            hwm = Integer.MAX_VALUE;

            curr = null;
            owner.sendCancel(nodeId, batchId);
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
                owner.flushFromBuffer();
        }
    }
}
