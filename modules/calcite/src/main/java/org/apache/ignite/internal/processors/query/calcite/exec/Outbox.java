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
import org.apache.ignite.internal.processors.query.calcite.exchange.ExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.util.typedef.F;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12448
 */
public class Outbox<T> extends AbstractNode<T> implements SingleNode<T>, Sink<T>, AutoCloseable {
    /** */
    private final Map<UUID, Buffer> perNodeBuffers = new HashMap<>();

    /** */
    private final long exchangeId;

    /** */
    private final DestinationFunction function;

    /** */
    private boolean registered;

    /**
     * @param ctx Execution context.
     * @param function Destination function.
     */
    public Outbox(ExecutionContext ctx, long exchangeId, Node<T> input, DestinationFunction function) {
        super(ctx, input);
        this.exchangeId = exchangeId;
        this.function = function;

        link();
    }

    public UUID queryId() {
        return context().queryId();
    }

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
        context().execute(this::requestInternal);
    }

    /** */
    private void requestInternal() {
        if (!registered) {
            exchange().register(this);

            registered = true;
        }

        super.request();
    }

    /** {@inheritDoc} */
    @Override public void target(Sink<T> sink) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Sink<T> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean push(T row) {
        List<UUID> nodes = function.destination(row);

        if (F.isEmpty(nodes))
            return true;

        List<Buffer> buffers = new ArrayList<>(nodes.size());

        for (UUID node : nodes) {
            Buffer dest = perNodeBuffers.computeIfAbsent(node, this::createBuffer);

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
        for (UUID node : function.targets())
            perNodeBuffers.computeIfAbsent(node, this::createBuffer).end();

        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        exchange().unregister(this);
    }

    /** */
    private void send(UUID nodeId, int batchId, List<?> rows) {
        exchange().send(queryId(), exchangeId, nodeId, batchId, rows);
    }

    /** */
    private ExchangeProcessor exchange() {
        return context().plannerContext().exchangeProcessor();
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

            curr = new ArrayList<>(ExchangeProcessor.BATCH_SIZE + 1); // extra space for end marker;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(Object row) {
            assert ready();

            if (curr.size() == ExchangeProcessor.BATCH_SIZE) {
                int batchId;

                synchronized (this) {
                    batchId = ++hwm;
                }

                owner.send(nodeId, batchId, curr);

                curr = new ArrayList<>(ExchangeProcessor.BATCH_SIZE + 1); // extra space for end marker;
            }

            curr.add(row);
        }

        /**
         * Signals data is over.
         */
        public void end() {
            int batchId;

            synchronized (this) {
                batchId = hwm + 1;
                hwm = Integer.MAX_VALUE;
            }

            curr.add(EndMarker.INSTANCE);
            owner.send(nodeId, batchId, curr);
            curr = null;
        }

        /**
         * Checks whether there is a place for a new row.
         *
         * @return {@code True} is it possible to add a row to a batch.
         */
        private boolean ready() {
            assert curr != null;

            boolean canSend;

            synchronized (this) {
                canSend = hwm - lwm < ExchangeProcessor.PER_NODE_BATCH_COUNT;
            }

            return canSend || curr.size() < ExchangeProcessor.BATCH_SIZE;
        }

        /**
         * Callback method.
         *
         * @param id batch ID.
         */
        private void onAcknowledge(int id) {
            boolean request = false;

            synchronized (this) {
                if (lwm < id) {
                    request = hwm - lwm == ExchangeProcessor.PER_NODE_BATCH_COUNT;

                    lwm = id;
                }
            }

            if (request)
                owner.request();
        }
    }
}
