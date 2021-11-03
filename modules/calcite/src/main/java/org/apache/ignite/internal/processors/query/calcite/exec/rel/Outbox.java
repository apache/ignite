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

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.IN_BUFFER_SIZE;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.trait.Destination;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * A part of exchange.
 */
public class Outbox<RowT> extends AbstractNode<RowT> implements Mailbox<RowT>, SingleNode<RowT>, Downstream<RowT> {
    /**
     *
     */
    private final ExchangeService exchange;

    /**
     *
     */
    private final MailboxRegistry registry;

    /**
     *
     */
    private final long exchangeId;

    /**
     *
     */
    private final long targetFragmentId;

    /**
     *
     */
    private final Destination<RowT> dest;

    /**
     *
     */
    private final Deque<RowT> inBuf = new ArrayDeque<>(inBufSize);

    /**
     *
     */
    private final Map<String, Buffer> nodeBuffers = new HashMap<>();

    /**
     *
     */
    private int waiting;

    /**
     * @param ctx              Execution context.
     * @param exchange         Exchange service.
     * @param registry         Mailbox registry.
     * @param exchangeId       Exchange ID.
     * @param targetFragmentId Target fragment ID.
     * @param dest             Destination.
     */
    public Outbox(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            ExchangeService exchange,
            MailboxRegistry registry,
            long exchangeId,
            long targetFragmentId,
            Destination<RowT> dest
    ) {
        super(ctx, rowType);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.dest = dest;
    }

    /** {@inheritDoc} */
    @Override
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * callback method.
     *
     * @param nodeId  Target ID.
     * @param batchId Batch ID.
     */
    public void onAcknowledge(String nodeId, int batchId) throws Exception {
        assert nodeBuffers.containsKey(nodeId);

        checkState();

        nodeBuffers.get(nodeId).acknowledge(batchId);
    }

    /**
     *
     */
    public void init() {
        try {
            checkState();

            flush();
        } catch (Throwable t) {
            onError(t);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowCnt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert waiting > 0;

        checkState();

        waiting--;

        inBuf.add(row);

        flush();
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** {@inheritDoc} */
    @Override
    protected void onErrorInternal(Throwable e) {
        log.error("Error occurred during execution", e);

        try {
            sendError(e);
        } catch (IgniteInternalCheckedException ex) {
            log.error("Error occurred during send error message", e);
        } finally {
            Commons.closeQuiet(this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        registry.unregister(this);

        // Send cancel message for the Inbox to close Inboxes created by batch message race.
        for (String node : dest.targets()) {
            getOrCreateBuffer(node).close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /**
     *
     */
    private void sendBatch(String nodeId, int batchId, boolean last, List<RowT> rows) throws IgniteInternalCheckedException {
        exchange.sendBatch(nodeId, queryId(), targetFragmentId, exchangeId, batchId, last, rows);
    }

    /**
     *
     */
    private void sendError(Throwable err) throws IgniteInternalCheckedException {
        exchange.sendError(context().originatingNodeId(), queryId(), fragmentId(), err);
    }

    /**
     *
     */
    private void sendInboxClose(String nodeId) {
        try {
            exchange.closeInbox(nodeId, queryId(), targetFragmentId, exchangeId);
        } catch (IgniteInternalCheckedException e) {
            log.error("Failed to send cancel message", e);
        }
    }

    /**
     *
     */
    private Buffer getOrCreateBuffer(String nodeId) {
        return nodeBuffers.computeIfAbsent(nodeId, this::createBuffer);
    }

    /**
     *
     */
    private Buffer createBuffer(String nodeId) {
        return new Buffer(nodeId);
    }

    /**
     *
     */
    private void flush() throws Exception {
        while (!inBuf.isEmpty()) {
            checkState();

            Collection<Buffer> buffers = dest.targets(inBuf.peek()).stream()
                    .map(this::getOrCreateBuffer)
                    .collect(Collectors.toList());

            assert !nullOrEmpty(buffers);

            if (!buffers.stream().allMatch(Buffer::ready)) {
                return;
            }

            RowT row = inBuf.remove();

            for (Buffer dest : buffers) {
                dest.add(row);
            }
        }

        assert inBuf.isEmpty();

        if (waiting == 0) {
            source().request(waiting = IN_BUFFER_SIZE);
        } else if (waiting == -1) {
            for (String node : dest.targets()) {
                getOrCreateBuffer(node).end();
            }
        }
    }

    /**
     *
     */
    public void onNodeLeft(String nodeId) {
        if (nodeId.equals(context().originatingNodeId())) {
            context().execute(this::close, this::onError);
        }
    }

    /**
     *
     */
    private final class Buffer {
        /**
         *
         */
        private final String nodeId;

        /**
         *
         */
        private int hwm = -1;

        /**
         *
         */
        private int lwm = -1;

        /**
         *
         */
        private List<RowT> curr;

        /**
         *
         */
        private Buffer(String nodeId) {
            this.nodeId = nodeId;

            curr = new ArrayList<>(IO_BATCH_SIZE);
        }

        /**
         * Checks whether there is a place for a new row.
         *
         * @return {@code True} is it possible to add a row to a batch.
         */
        private boolean ready() {
            if (hwm == Integer.MAX_VALUE) {
                return false;
            }

            return curr.size() < IO_BATCH_SIZE || hwm - lwm < IO_BATCH_CNT;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row.
         */
        public void add(RowT row) throws IgniteInternalCheckedException {
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
        public void end() throws IgniteInternalCheckedException {
            if (hwm == Integer.MAX_VALUE) {
                return;
            }

            int batchId = hwm + 1;
            hwm = Integer.MAX_VALUE;

            List<RowT> tmp = curr;
            curr = null;

            sendBatch(nodeId, batchId, true, tmp);
        }

        /**
         * Callback method.
         *
         * @param id batch ID.
         */
        private void acknowledge(int id) throws Exception {
            if (lwm > id) {
                return;
            }

            boolean readyBefore = ready();

            lwm = id;

            if (!readyBefore && ready()) {
                flush();
            }
        }

        /**
         *
         */
        public void close() {
            final int currBatchId = hwm;

            if (hwm == Integer.MAX_VALUE) {
                return;
            }

            hwm = Integer.MAX_VALUE;

            curr = null;

            if (currBatchId >= 0) {
                sendInboxClose(nodeId);
            }
        }
    }
}
