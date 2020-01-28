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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.util.typedef.F;

/**
 * A part of exchange.
 */
public class Outbox<T> extends AbstractNode<T> implements SingleNode<T>, Sink<T>, AutoCloseable {
    /** */
    private final ExchangeService exchange;

    /** */
    private final long exchangeId;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long targetFragmentId;

    /** */
    private final Map<UUID, Buffer> perNodeBuffers = new HashMap<>();

    /** */
    private final Destination destination;

    /** */
    private boolean cancelled;

    /**
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param ctx Execution context.
     * @param targetFragmentId Target fragment ID.
     * @param exchangeId Exchange ID.
     * @param input Input node.
     * @param destination Destination.
     */
    public Outbox(ExchangeService exchange, MailboxRegistry registry, ExecutionContext ctx, long targetFragmentId, long exchangeId, Node<T> input, Destination destination) {
        super(ctx, input);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.destination = destination;

        link();
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
        perNodeBuffers.get(nodeId).onAcknowledge(batchId);
    }

    /** {@inheritDoc} */
    @Override public void request() {
        checkThread();

        if (context().cancelled())
            cancelInternal();
        else
            input().request();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();

        context().setCancelled();
        cancelInternal();
    }

    /** */
    private void cancelInternal() {
        if (cancelled)
            return;

        try {
            perNodeBuffers.values().forEach(Buffer::cancel);
            input().cancel();
        }
        finally {
            cancelled = true;
            close();
        }
    }

    /** {@inheritDoc} */
    @Override public void target(Sink<T> sink) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public Sink<T> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean push(T row) {
        List<UUID> nodes = destination.targets(row);

        assert !F.isEmpty(nodes);

        List<Buffer> buffers = new ArrayList<>(nodes.size());

        for (UUID node : nodes) {
            Buffer dest = getOrCreateBuffer(node);

            if (!dest.ready())
                return false;

            buffers.add(dest);
        }

        for (Buffer dest : buffers)
            dest.add(row);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void end() {
        for (UUID node : destination.targets())
            getOrCreateBuffer(node).end();

        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        registry.unregister(this);
    }

    /** */
    private void sendBatch(UUID nodeId, int batchId, List<?> rows) {
        exchange.sendBatch(this, nodeId, queryId(), targetFragmentId, exchangeId, batchId, rows);
    }

    /** */
    private void sendCancel(UUID nodeId, int batchId) {
        exchange.cancel(this, nodeId, queryId(), targetFragmentId, exchangeId, batchId);
    }

    /** */
    private Buffer getOrCreateBuffer(UUID nodeId) {
        return perNodeBuffers.computeIfAbsent(nodeId, this::createBuffer);
    }

    /** */
    private Buffer createBuffer(UUID nodeId) {
        return new Buffer(nodeId, this);
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

            curr = new ArrayList<>(ExchangeService.BATCH_SIZE + 1); // extra space for end marker;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(Object row) {
            assert ready();

            if (curr.size() == ExchangeService.BATCH_SIZE) {
                owner.sendBatch(nodeId, ++hwm, curr);
                curr = new ArrayList<>(ExchangeService.BATCH_SIZE + 1); // extra space for end marker;
            }

            curr.add(row);
        }

        /**
         * Signals data is over.
         */
        public void end() {
            int batchId = hwm + 1;
            hwm = Integer.MAX_VALUE;

            List<Object> tmp = curr;
            curr = null;

            tmp.add(EndMarker.INSTANCE);
            owner.sendBatch(nodeId, batchId, tmp);
        }

        public void cancel() {
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
            if (curr == null)
                throw new AssertionError();

            return hwm != Integer.MAX_VALUE
                && hwm - lwm < ExchangeService.PER_NODE_BATCH_COUNT
                || curr.size() < ExchangeService.BATCH_SIZE;
        }

        /**
         * Callback method.
         *
         * @param id batch ID.
         */
        private void onAcknowledge(int id) {
            if (lwm > id)
                return;

            boolean request = hwm - lwm == ExchangeService.PER_NODE_BATCH_COUNT;

            lwm = id;

            if (request)
                owner.request();
        }
    }
}
