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
import org.apache.ignite.internal.processors.query.calcite.exec.EndMarker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.jetbrains.annotations.NotNull;

/**
 * A part of exchange.
 */
public class Inbox<T> extends AbstractNode<T> implements SingleNode<T>, AutoCloseable {
    /** */
    private final ExchangeService exchange;

    /** */
    private final MailboxRegistry registry;

    /** */
    private final long exchangeId;

    /** */
    private final long sourceFragmentId;

    /** */
    private final Map<UUID, Buffer> perNodeBuffers;

    /** */
    private Collection<UUID> nodes;

    /** */
    private Comparator<T> comparator;

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
     * @param sourceFragmentId Source fragment ID.
     */
    public Inbox(ExecutionContext ctx, ExchangeService exchange, MailboxRegistry registry, long exchangeId, long sourceFragmentId) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;

        this.sourceFragmentId = sourceFragmentId;
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
     * @param comparator Optional comparator for merge exchange.
     */
    public void init(ExecutionContext ctx, Collection<UUID> nodes, Comparator<T> comparator) {
        this.ctx = ctx;
        this.nodes = nodes;
        this.comparator = comparator;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert nodes != null;
        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (buffers == null) {
            nodes.forEach(this::getOrCreateBuffer);
            buffers = new ArrayList<>(perNodeBuffers.values());

            assert buffers.size() == nodes.size();
        }

        if (!inLoop)
            context().execute(this::pushInternal);
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
    @Override protected Downstream<T> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<T>> sources) {
        throw new UnsupportedOperationException();
    }

    /**
     * Pushes a batch into a buffer.
     *
     * @param source Source node.
     * @param batchId Batch ID.
     * @param rows Rows.
     */
    public void onBatchReceived(UUID source, int batchId, List<?> rows) {
        checkThread();

        Buffer buffer = getOrCreateBuffer(source);

        boolean waitingBefore = buffer.check() == State.WAITING;

        buffer.offer(batchId, rows);

        if (requested > 0 && waitingBefore && buffer.check() != State.WAITING)
            pushInternal();
    }

    /** */
    private void pushInternal() {
        assert downstream != null;

        inLoop = true;
        try {
            if (comparator != null)
                pushOrdered();
            else
                pushUnordered();
        }
        catch (Exception e) {
            downstream.onError(e);
            close();
        }
        finally {
            inLoop = false;
        }
    }

    /** */
    private void pushOrdered() throws IgniteCheckedException {
         PriorityQueue<Pair<T, Buffer>> heap =
            new PriorityQueue<>(buffers.size(), Map.Entry.comparingByKey(comparator));

        Iterator<Buffer> it = buffers.iterator();

        while (it.hasNext()) {
            Buffer buffer = it.next();

            switch (buffer.check()) {
                case END:
                    it.remove();

                    break;
                case READY:
                    heap.offer(Pair.of((T)buffer.peek(), buffer));

                    break;
                case WAITING:

                    return;
            }
        }

        while (requested > 0 && !heap.isEmpty()) {
            if (context().cancelled())
                return;

            Buffer buffer = heap.poll().right;

            requested--;
            downstream.push((T)buffer.remove());

            switch (buffer.check()) {
                case END:
                    buffers.remove(buffer);

                    break;
                case READY:
                    heap.offer(Pair.of((T)buffer.peek(), buffer));

                    break;
                case WAITING:

                    return;
            }
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

        while (requested > 0 && !buffers.isEmpty()) {
            if (context().cancelled())
                return;

            Buffer buffer = buffers.get(idx);

            switch (buffer.check()) {
                case END:
                    buffers.remove(idx--);

                    break;
                case READY:
                    noProgress = 0;
                    requested--;
                    downstream.push((T)buffer.remove());

                    break;
                case WAITING:
                    if (++noProgress >= buffers.size())
                        return;

                    break;
            }

            if (++idx == buffers.size())
                idx = 0;
        }

        if (requested > 0 && buffers.isEmpty()) {
            downstream.end();
            requested = 0;

            close();
        }
    }

    /** */
    private void acknowledge(UUID nodeId, int batchId) throws IgniteCheckedException {
        exchange.acknowledge(nodeId, queryId(), sourceFragmentId, exchangeId, batchId);
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
    private static final class Batch implements Comparable<Batch> {
        /** */
        private final int batchId;

        /** */
        private final List<?> rows;

        /** */
        private int idx;

        /** */
        private Batch(int batchId, List<?> rows) {
            this.batchId = batchId;
            this.rows = rows;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Batch batch = (Batch) o;

            return batchId == batch.batchId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return batchId;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Inbox.Batch o) {
            return Integer.compare(batchId, o.batchId);
        }
    }

    /** */
    private enum State {
        END, READY, WAITING
    }

    /** */
    private static final class Buffer {
        /** */
        private static final Batch WAITING = new Batch(0, null);

        /** */
        private static final Batch END = new Batch(0, null);

        /** */
        private final Inbox<?> owner;

        /** */
        private final UUID nodeId;

        /** */
        private int lastEnqueued = -1;

        /** */
        private final PriorityQueue<Batch> batches = new PriorityQueue<>(IO_BATCH_CNT);

        /** */
        private Batch curr = WAITING;

        /** */
        private Buffer(UUID nodeId, Inbox<?> owner) {
            this.nodeId = nodeId;
            this.owner = owner;
        }

        /** */
        private void offer(int id, List<?> rows) {
            batches.offer(new Batch(id, rows));
        }

        /** */
        private Batch pollBatch() {
            if (batches.isEmpty() || batches.peek().batchId != lastEnqueued + 1)
                return WAITING;

            Batch batch = batches.poll();

            assert batch != null && batch.batchId == lastEnqueued + 1;

            lastEnqueued = batch.batchId;

            return batch;
        }

        /** */
        private State check() {
            if (curr == END)
                return State.END;

            if (curr == WAITING && (curr = pollBatch()) == WAITING)
                return State.WAITING;

            if (curr.rows.get(curr.idx) == EndMarker.INSTANCE) {
                curr = END;

                return State.END;
            }

            return State.READY;
        }

        /** */
        private Object peek() {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert curr.rows.get(curr.idx) != EndMarker.INSTANCE;

            return curr.rows.get(curr.idx);
        }

        /** */
        private Object remove() throws IgniteCheckedException {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert curr.rows.get(curr.idx) != EndMarker.INSTANCE;

            Object row = curr.rows.set(curr.idx++, null);

            if (curr.idx == curr.rows.size()) {
                owner.acknowledge(nodeId, curr.batchId);

                curr = pollBatch();
            }

            return row;
        }
    }
}
