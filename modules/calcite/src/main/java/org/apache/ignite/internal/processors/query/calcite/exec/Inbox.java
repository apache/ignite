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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.util.Pair;
import org.jetbrains.annotations.NotNull;

/**
 * A part of exchange.
 */
public class Inbox<T> extends AbstractNode<T> implements SingleNode<T>, AutoCloseable {
    /** */
    private final long exchangeId;

    /** */
    private final long sourceFragmentId;

    /** */
    private final Map<UUID, Buffer> perNodeBuffers;

    /** */
    private Collection<UUID> sources;

    /** */
    private Comparator<T> comparator;

    /** */
    private List<Buffer> buffers;

    /** */
    private boolean end;

    /** */
    private volatile boolean initDone;

    /**
     * @param ctx Execution context.
     * @param exchangeId Exchange ID.
     */
    public Inbox(ExecutionContext ctx, long sourceFragmentId, long exchangeId) {
        super(ctx);

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
     * @param sources Source nodes.
     * @param comparator Optional comparator for merge exchange.
     */
    public void init(ExecutionContext ctx, Collection<UUID> sources, Comparator<T> comparator) {
        this.comparator = comparator;
        this.sources = sources;

        context(ctx);

        initDone = true;
    }

    /** {@inheritDoc} */
    @Override public void request() {
        checkThread();
        pushInternal();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        checkThread();
        context().setCancelled();
        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        context().mailboxRegistry().unregister(this);
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

        if (perNodeBuffers.computeIfAbsent(source, this::createBuffer).add(batchId, rows))
            pushInternal();
    }

    /** {@inheritDoc} */
    @Override public Sink<T> sink(int idx) {
        throw new UnsupportedOperationException();
    }

    /** */
    private void pushInternal() {
        checkThread();

        if (context().cancelled())
            close();
        else if (!end && prepareBuffers()) {
            if (comparator != null)
                pushOrdered();
            else
                pushUnordered();
        }
    }

    /** */
    private boolean prepareBuffers() {
        if (!initDone)
            return false;

        assert sources != null;

        if (buffers != null)
            return true;

        // awaits till all sources sent a first bunch of batches
        if (perNodeBuffers.size() != sources.size())
            return false;

        buffers = new ArrayList<>(perNodeBuffers.values());

        return true;
    }

    /** */
    private void pushOrdered() {
        PriorityQueue<Pair<T, Buffer>> heap = new PriorityQueue<>(buffers.size(), Map.Entry.comparingByKey(comparator));

        ListIterator<Buffer> it = buffers.listIterator();

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

        Sink<T> target = target();

        while (!heap.isEmpty()) {
            Pair<T, Buffer> pair = heap.poll();

            T row = pair.left; Buffer buffer = pair.right;

            if (!target.push(row))
                return;

            buffer.remove();

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

        end = true;
        target.end();
        close();
    }

    /** */
    private void pushUnordered() {
        int size = buffers.size();

        if (size <= 0 && !end)
            throw new AssertionError("size=" + size + ", end=" + end);

        int idx = ThreadLocalRandom.current().nextInt(size);
        int noProgress = 0;

        Sink<T> target = target();

        while (size > 0) {
            Buffer buffer = buffers.get(idx);

            switch (buffer.check()) {
                case END:
                    buffers.remove(idx);

                    if (idx == --size)
                        idx = 0;

                    continue;
                case READY:
                    if (!target.push((T)buffer.peek()))
                        return;

                    buffer.remove();
                    noProgress = 0;

                    break;
                case WAITING:
                    if (++noProgress >= size)
                        return;

                    break;
            }

            if (++idx == size)
                idx = 0;
        }

        end = true;
        target.end();
        close();
    }

    /** */
    private void acknowledge(UUID nodeId, int batchId) {
        context().exchange().acknowledge(this, nodeId, queryId(), sourceFragmentId, exchangeId, batchId);
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
        private final PriorityQueue<Batch> batches = new PriorityQueue<>(ExchangeService.PER_NODE_BATCH_COUNT);

        /** */
        private Batch curr = WAITING;

        /** */
        private Buffer(UUID nodeId, Inbox<?> owner) {
            this.nodeId = nodeId;
            this.owner = owner;
        }

        /** */
        private boolean add(int id, List<?> rows) {
            batches.offer(new Batch(id, rows));

            return curr == WAITING && batches.peek().batchId == lastEnqueued + 1;
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

            if (curr == WAITING)
                curr = pollBatch();

            if (curr == WAITING)
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
        private Object remove() {
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
