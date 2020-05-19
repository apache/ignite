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
    private final ExchangeService<Row> exchange;

    /** */
    private final MailboxRegistry<Row> registry;

    /** */
    private final long exchangeId;

    /** */
    private final long srcFragmentId;

    /** */
    private final Map<UUID, Buffer<Row>> perNodeBuffers;

    /** */
    private Collection<UUID> nodes;

    /** */
    private Comparator<Row> comp;

    /** */
    private List<Buffer<Row>> buffers;

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
        ExchangeService<Row> exchange,
        MailboxRegistry<Row> registry,
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
     * @param nodes Source nodes.
     * @param comp Optional comparator for merge exchange.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public void init(Collection<UUID> nodes, Comparator<Row> comp) {
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
     * @param rows Rows.
     */
    public void onBatchReceived(UUID src, int batchId, List<Row> rows) {
        checkThread();

        Buffer<Row> buf = getOrCreateBuffer(src);

        boolean waitingBefore = buf.check() == State.WAITING;

        buf.offer(batchId, rows);

        if (requested > 0 && waitingBefore && buf.check() != State.WAITING)
            pushInternal();
    }

    /** */
    private void pushInternal() {
        assert downstream != null;

        inLoop = true;
        try {
            if (comp != null)
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
         PriorityQueue<Pair<Row, Buffer<Row>>> heap =
            new PriorityQueue<>(buffers.size(), Map.Entry.comparingByKey(comp));

        Iterator<Buffer<Row>> it = buffers.iterator();

        while (it.hasNext()) {
            Buffer<Row> buf = it.next();

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

        while (requested > 0 && !heap.isEmpty()) {
            if (context().cancelled())
                return;

            Buffer<Row> buf = heap.poll().right;

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

            Buffer<Row> buf = buffers.get(idx);

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

        if (requested > 0 && buffers.isEmpty()) {
            downstream.end();
            requested = 0;

            close();
        }
    }

    /** */
    private void acknowledge(UUID nodeId, int batchId) throws IgniteCheckedException {
        exchange.acknowledge(nodeId, queryId(), srcFragmentId, exchangeId, batchId);
    }

    /** */
    private Buffer<Row> getOrCreateBuffer(UUID nodeId) {
        return perNodeBuffers.computeIfAbsent(nodeId, this::createBuffer);
    }

    /** */
    private Buffer<Row> createBuffer(UUID nodeId) {
        return new Buffer<>(nodeId, this);
    }

    /** */
    private static final class Batch<Row> implements Comparable<Batch<Row>> {
        /** */
        private final int batchId;

        /** */
        private final List<Row> rows;

        /** */
        private int idx;

        /** */
        private Batch(int batchId, List<Row> rows) {
            this.batchId = batchId;
            this.rows = rows;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Batch<?> batch = (Batch<?>) o;

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
        /** */
        END,

        /** */
        READY,

        /** */
        WAITING
    }

    /** */
    private static final class Buffer<Row> {
        /** */
        private static final Batch<?> WAITING = new Batch<>(0, null);

        /** */
        private static final Batch<?> END = new Batch<>(0, null);

        /** */
        private final Inbox<Row> owner;

        /** */
        private final UUID nodeId;

        /** */
        private int lastEnqueued = -1;

        /** */
        private final PriorityQueue<Batch<Row>> batches = new PriorityQueue<>(IO_BATCH_CNT);

        /** */
        private Batch<Row> curr = waitingMark();

        /** */
        private Buffer(UUID nodeId, Inbox<Row> owner) {
            this.nodeId = nodeId;
            this.owner = owner;
        }

        /** */
        private void offer(int id, List<Row> rows) {
            batches.offer(new Batch<>(id, rows));
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

            if (needToWait())
                return State.WAITING;

            if (isLastRow()) {
                curr = endMark();

                return State.END;
            }

            return State.READY;
        }

        /** */
        private Row peek() {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert !owner.hnd.isEndMarker(curr.rows.get(curr.idx));

            return curr.rows.get(curr.idx);
        }

        /** */
        private Row remove() throws IgniteCheckedException {
            assert curr != null;
            assert curr != WAITING;
            assert curr != END;
            assert !owner.hnd.isEndMarker(curr.rows.get(curr.idx));

            Row row = curr.rows.set(curr.idx++, null);

            if (curr.idx == curr.rows.size()) {
                owner.acknowledge(nodeId, curr.batchId);

                curr = pollBatch();
            }

            return row;
        }

        /** */
        private boolean finished() {
            return curr == END;
        }

        /** */
        private boolean needToWait() {
            return curr == WAITING && (curr = pollBatch()) == WAITING;
        }

        /** */
        private boolean isLastRow() {
            return owner.hnd.isEndMarker(curr.rows.get(curr.idx));
        }

        /** */
        private static <T> T endMark() {
            return (T) END;
        }

        /** */
        private static <T> T waitingMark() {
            return (T) WAITING;
        }
    }
}
