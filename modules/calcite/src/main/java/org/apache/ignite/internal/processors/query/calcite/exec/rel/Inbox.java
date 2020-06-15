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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.jetbrains.annotations.NotNull;

/**
 * A part of exchange.
 */
public class Inbox<Row> extends AbstractNode<Row> implements SingleNode<Row>, AutoCloseable {
    /** */
    private final ExchangeService exchange;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long exchangeId;

    /** */
    private final long srcFragmentId;

    /** */
    private final Map<UUID, Buffer> perNodeBuffers;

    /** */
    private Collection<UUID> nodes;

    /** */
    private Comparator<Row> comp;

    /** */
    private List<Buffer> buffers;

    /** */
    private int requested;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param exchangeId Exchange ID.
     * @param srcFragmentId Source fragment ID.
     */
    public Inbox(
        ExecutionContext<Row> ctx,
        ExchangeService exchange,
        MailboxRegistry registry,
        long exchangeId,
        long srcFragmentId
    ) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;

        this.srcFragmentId = srcFragmentId;
        this.exchangeId = exchangeId;

        perNodeBuffers = new HashMap<>();
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
     * Inits this Inbox.
     *
     * @param ctx Execution context.
     * @param nodes Source nodes.
     * @param comp Optional comparator for merge exchange.
     */
    public void init(ExecutionContext<Row> ctx, Collection<UUID> nodes, Comparator<Row> comp) {
        // It's important to set proper context here because
        // because the one, that is created on a first message
        // recived doesn't have all context variables in place.
        this.ctx = ctx;
        this.nodes = nodes;
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert nodes != null;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (buffers == null) {
            nodes.forEach(this::getOrCreateBuffer);
            buffers = new ArrayList<>(perNodeBuffers.values());

            assert buffers.size() == nodes.size();
        }

        if (!inLoop)
            context().execute(this::push);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();
        context().markCancelled();
        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        registry.unregister(this);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        throw new UnsupportedOperationException();
    }

    /**
     * Pushes a batch into a buffer.
     *
     * @param src Source node.
     * @param batchId Batch ID.
     * @param last Last batch flag.
     * @param rows Rows.
     */
    public void onBatchReceived(UUID src, int batchId, boolean last, List<Row> rows) {
        checkThread();

        Buffer buf = getOrCreateBuffer(src);

        boolean waitingBefore = buf.check() == State.WAITING;

        buf.offer(batchId, last, rows);

        if (requested > 0 && waitingBefore && buf.check() != State.WAITING)
            push();
    }

    /** */
    private void push() {
        assert downstream != null;

        try {
            if (comp != null)
                pushOrdered();
            else
                pushUnordered();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void pushOrdered() throws IgniteCheckedException {
         PriorityQueue<Pair<Row, Buffer>> heap =
            new PriorityQueue<>(buffers.size(), Map.Entry.comparingByKey(comp));

        Iterator<Buffer> it = buffers.iterator();

        while (it.hasNext()) {
            Buffer buf = it.next();

            switch (buf.check()) {
                case END:
                    it.remove();

                    break;
                case READY:
                    heap.offer(Pair.of(buf.peek(), buf));

                    break;
                case WAITING:

                    return;
            }
        }

        inLoop = true;
        try {
            while (requested > 0 && !heap.isEmpty()) {
                if (context().cancelled())
                    return;

                Buffer buf = heap.poll().right;

                requested--;
                downstream.push(buf.remove());

                switch (buf.check()) {
                    case END:
                        buffers.remove(buf);

                        break;
                    case READY:
                        heap.offer(Pair.of(buf.peek(), buf));

                        break;
                    case WAITING:

                        return;
                }
            }
        }
        finally {
            inLoop = false;
        }

        if (requested > 0 && heap.isEmpty()) {
            assert buffers.isEmpty();

            downstream.end();
            requested = 0;

            close();
        }
    }

    /** */
    private void pushUnordered() throws IgniteCheckedException {
        int idx = 0, noProgress = 0;

        inLoop = true;
        try {
            while (requested > 0 && !buffers.isEmpty()) {
                if (context().cancelled())
                    return;

                Buffer buf = buffers.get(idx);

                switch (buf.check()) {
                    case END:
                        buffers.remove(idx--);

                        break;
                    case READY:
                        noProgress = 0;
                        requested--;
                        downstream.push(buf.remove());

                        break;
                    case WAITING:
                        if (++noProgress >= buffers.size())
                            return;

                        break;
                }

                if (++idx == buffers.size())
                    idx = 0;
            }
        }
        finally {
            inLoop = false;
        }

        if (requested > 0 && buffers.isEmpty()) {
            downstream.end();
            requested = 0;

            close();
        }
    }

    /** */
    private void onError(Exception e) {
        checkThread();

        assert downstream != null;

        try {
            downstream.onError(e);
        }
        finally {
            close();
        }
    }

    /** */
    private void acknowledge(UUID nodeId, int batchId) throws IgniteCheckedException {
        exchange.acknowledge(nodeId, queryId(), srcFragmentId, exchangeId, batchId);
    }

    /** */
    private Buffer getOrCreateBuffer(UUID nodeId) {
        return perNodeBuffers.computeIfAbsent(nodeId, this::createBuffer);
    }

    /** */
    private Buffer createBuffer(UUID nodeId) {
        return new Buffer(nodeId);
    }

    /** */
    private static final class Batch<Row> implements Comparable<Batch<Row>> {
        /** */
        private final int batchId;

        /** */
        private final boolean last;

        /** */
        private final List<Row> rows;

        /** */
        private int idx;

        /** */
        private Batch(int batchId, boolean last, List<Row> rows) {
            this.batchId = batchId;
            this.last = last;
            this.rows = rows;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Batch<Row> batch = (Batch<Row>) o;

            return batchId == batch.batchId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return batchId;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Inbox.Batch<Row> o) {
            return Integer.compare(batchId, o.batchId);
        }
    }

    /** */
    private enum State {
        /** */
        END,

        /** */
        READY,

        /** */
        WAITING
    }

    /** */
    private static final Batch<?> WAITING = new Batch<>(0, false, null);

    /** */
    private static final Batch<?> END = new Batch<>(0, false, null);

    /** */
    private final class Buffer {
        /** */
        private final UUID nodeId;

        /** */
        private int lastEnqueued = -1;

        /** */
        private final PriorityQueue<Batch<Row>> batches = new PriorityQueue<>(IO_BATCH_CNT);

        /** */
        private Batch<Row> curr = waitingMark();

        /** */
        private Buffer(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** */
        private void offer(int id, boolean last, List<Row> rows) {
            batches.offer(new Batch<>(id, last, rows));
        }

        /** */
        private Batch<Row> pollBatch() {
            if (batches.isEmpty() || batches.peek().batchId != lastEnqueued + 1)
                return waitingMark();

            Batch<Row> batch = batches.poll();

            assert batch != null && batch.batchId == lastEnqueued + 1;

            lastEnqueued = batch.batchId;

            return batch;
        }

        /** */
        private State check() {
            if (finished())
                return State.END;

            if (waiting())
                return State.WAITING;

            if (isEnd()) {
                curr = finishedMark();

                return State.END;
            }

            return State.READY;
        }

        /** */
        private Row peek() {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert !isEnd();

            return curr.rows.get(curr.idx);
        }

        /** */
        private Row remove() throws IgniteCheckedException {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert !isEnd();

            Row row = curr.rows.set(curr.idx++, null);

            if (curr.idx == curr.rows.size()) {
                acknowledge(nodeId, curr.batchId);

                if (!isEnd())
                    curr = pollBatch();
            }

            return row;
        }

        /** */
        private boolean finished() {
            return curr == END;
        }

        /** */
        private boolean waiting() {
            return curr == WAITING && (curr = pollBatch()) == WAITING;
        }

        /** */
        private boolean isEnd() {
            return curr.last && curr.idx == curr.rows.size();
        }

        /** */
        private Batch<Row> finishedMark() {
            return (Batch<Row>) END;
        }

        /** */
        private Batch<Row> waitingMark() {
            return (Batch<Row>) WAITING;
        }
    }
}
