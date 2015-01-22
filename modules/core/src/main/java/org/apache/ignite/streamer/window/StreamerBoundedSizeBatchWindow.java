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

package org.apache.ignite.streamer.window;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.streamer.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Window that is bounded by size and accumulates events to batches.
 */
public class StreamerBoundedSizeBatchWindow<E> extends StreamerWindowAdapter<E> {
    /** Max size. */
    private int batchSize;

    /** Min size. */
    private int maxBatches;

    /** Reference for queue and size. */
    private volatile QueueHolder holder;

    /** Enqueue lock. */
    private ReadWriteLock enqueueLock = new ReentrantReadWriteLock();

    /**
     * Gets maximum number of batches can be stored in window.
     *
     * @return Maximum number of batches for window.
     */
    public int getMaximumBatches() {
        return maxBatches;
    }

    /**
     * Sets maximum number of batches can be stored in window.
     *
     * @param maxBatches Maximum number of batches for window.
     */
    public void setMaximumBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

    /**
     * Gets batch size.
     *
     * @return Batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets batch size.
     *
     * @param batchSize Batch size.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /** {@inheritDoc} */
    @Override public void checkConfiguration() throws IgniteCheckedException {
        if (batchSize <= 0)
            throw new IgniteCheckedException("Failed to initialize window (batchSize size must be positive) " +
                "[windowClass=" + getClass().getSimpleName() +
                ", maximumBatches=" + maxBatches +
                ", batchSize=" + batchSize + ']');

        if (maxBatches < 0)
            throw new IgniteCheckedException("Failed to initialize window (maximumBatches cannot be negative) " +
                "[windowClass=" + getClass().getSimpleName() +
                ", maximumBatches=" + maxBatches +
                ", batchSize=" + batchSize + ']');
    }

    /** {@inheritDoc} */
    @Override protected void stop0() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void reset0() {
        ConcurrentLinkedDeque8<Batch> first = new ConcurrentLinkedDeque8<>();

        Batch b = new Batch(batchSize);

        ConcurrentLinkedDeque8.Node<Batch> n = first.offerLastx(b);

        b.node(n);

        holder = new QueueHolder(first, new AtomicInteger(1), new AtomicInteger());
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return holder.totalQueueSize().get();
    }

    /** {@inheritDoc} */
    @Override protected GridStreamerWindowIterator<E> iterator0() {
        final QueueHolder win = holder;

        final Iterator<Batch> batchIt = win.batchQueue().iterator();

        return new GridStreamerWindowIterator<E>() {
            /** Current batch iterator. */
            private ConcurrentLinkedDeque8.IteratorEx<E> curBatchIt;

            /** Next batch iterator. Will be null if no more batches available. */
            private ConcurrentLinkedDeque8.IteratorEx<E> nextBatchIt;

            /** Last returned value. */
            private E lastRet;

            {
                curBatchIt = batchIt.hasNext() ? batchIt.next().iterator() : null;
            }

            /** {@inheritDoc} */
            @SuppressWarnings("SimplifiableIfStatement")
            @Override public boolean hasNext() {
                if (curBatchIt != null) {
                    if (curBatchIt.hasNext())
                        return true;

                    return nextBatchIt != null && nextBatchIt.hasNext();
                }
                else
                    return false;
            }

            /** {@inheritDoc} */
            @Override public E next() {
                if (curBatchIt == null)
                    throw new NoSuchElementException();

                if (!curBatchIt.hasNext()) {
                    if (nextBatchIt != null) {
                        curBatchIt = nextBatchIt;

                        nextBatchIt = null;

                        lastRet = curBatchIt.next();
                    }
                    else
                        throw new NoSuchElementException();
                }
                else {
                    E next = curBatchIt.next();

                    // Moved to last element in batch - check for next iterator.
                    if (!curBatchIt.hasNext())
                        advanceBatch();

                    lastRet = next;
                }

                return lastRet;
            }

            /** {@inheritDoc} */
            @Nullable @Override public E removex() {
                if (curBatchIt == null)
                    throw new NoSuchElementException();

                if (curBatchIt.removex()) {
                    // Decrement global size if deleted.
                    win.totalQueueSize().decrementAndGet();

                    return lastRet;
                }
                else
                    return null;
            }

            /**
             * Moves to the next batch.
             */
            private void advanceBatch() {
                if (batchIt.hasNext()) {
                    Batch batch = batchIt.next();

                    nextBatchIt = batch.iterator();
                }
                else
                    nextBatchIt = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int evictionQueueSize() {
        QueueHolder win = holder;

        int oversizeCnt = Math.max(0, win.batchQueueSize().get() - maxBatches);

        Iterator<Batch> it = win.batchQueue().iterator();

        int size = 0;

        int idx = 0;

        while (it.hasNext()) {
            Batch batch = it.next();

            if (idx++ < oversizeCnt)
                size += batch.size();
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override protected boolean enqueue0(E evt) {
        try {
            return enqueueInternal(evt);
        }
        catch (IgniteInterruptedException ignored) {
            return false;
        }
    }

    /**
     * Enqueue event to window.
     *
     * @param evt Event to add.
     * @return {@code True} if event was added.
     *
     * @throws org.apache.ignite.IgniteInterruptedException If thread was interrupted.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private boolean enqueueInternal(E evt) throws IgniteInterruptedException {
        QueueHolder tup = holder;

        ConcurrentLinkedDeque8<Batch> evts = tup.batchQueue();
        AtomicInteger size = tup.batchQueueSize();

        while (true) {
            Batch last = evts.peekLast();

            if (last == null || !last.add(evt)) {
                // This call will ensure that last object is actually added to batch
                // before we add new batch to events queue.
                // If exception is thrown here, window will be left in consistent state.
                if (last != null)
                    last.finish();

                // Add new batch to queue in write lock.
                if (enqueueLock.writeLock().tryLock()) {
                    try {
                        Batch first0 = evts.peekLast();

                        if (first0 == last) {
                            Batch batch = new Batch(batchSize);

                            ConcurrentLinkedDeque8.Node<Batch> node = evts.offerLastx(batch);

                            batch.node(node);

                            size.incrementAndGet();

                            if (batch.removed() && evts.unlinkx(node))
                                size.decrementAndGet();
                        }
                    }
                    finally {
                        enqueueLock.writeLock().unlock();
                    }
                }
                else {
                    // Acquire read lock to wait for batch enqueue.
                    enqueueLock.readLock().lock();

                    enqueueLock.readLock().unlock();
                }
            }
            else {
                // Event was added, global size increment.
                tup.totalQueueSize().incrementAndGet();

                return true;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvicted0(int cnt) {
        QueueHolder tup = holder;

        ConcurrentLinkedDeque8<Batch> evts = tup.batchQueue();
        AtomicInteger size = tup.batchQueueSize();

        Collection<E> res = new ArrayList<>(cnt);

        while (true) {
            int curSize = size.get();

            if (curSize > maxBatches) {
                // Just peek the first batch.
                Batch first = evts.peekFirst();

                if (first != null) {
                    assert first.finished();

                    Collection<E> polled = first.pollNonBatch(cnt - res.size());

                    if (!polled.isEmpty())
                        res.addAll(polled);

                    if (first.isEmpty()) {
                        ConcurrentLinkedDeque8.Node<Batch> node = first.node();

                        first.markRemoved();

                        if (node != null && evts.unlinkx(node))
                            size.decrementAndGet();
                    }

                    if (res.size() == cnt)
                        break;
                }
                else
                    break;
            }
            else
                break;
        }

        // Removed entries, update global size.
        tup.totalQueueSize().addAndGet(-res.size());

        return res;
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvictedBatch0() {
        QueueHolder tup = holder;

        ConcurrentLinkedDeque8<Batch> evts = tup.batchQueue();
        AtomicInteger size = tup.batchQueueSize();

        while (true) {
            int curSize = size.get();

            if (curSize > maxBatches) {
                if (size.compareAndSet(curSize, curSize - 1)) {
                    Batch polled = evts.poll();

                    if (polled != null) {
                        assert polled.finished();

                        // Mark batch deleted for consistency.
                        polled.markRemoved();

                        Collection<E> polled0 = polled.shrink();

                        // Result of shrink is empty, must retry the poll.
                        if (!polled0.isEmpty()) {
                            // Update global size.
                            tup.totalQueueSize().addAndGet(-polled0.size());

                            return polled0;
                        }
                    }
                    else {
                        // Polled was zero, so we must restore counter and return.
                        size.incrementAndGet();

                        return Collections.emptyList();
                    }
                }
            }
            else
                return Collections.emptyList();
        }
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> dequeue0(int cnt) {
        QueueHolder tup = holder;

        ConcurrentLinkedDeque8<Batch> evts = tup.batchQueue();
        AtomicInteger size = tup.batchQueueSize();

        Collection<E> res = new ArrayList<>(cnt);

        while (true) {
            // Just peek the first batch.
            Batch first = evts.peekFirst();

            if (first != null) {
                Collection<E> polled = first.pollNonBatch(cnt - res.size());

                // We must check for finished before unlink as no elements
                // can be added to batch after it is finished.
                if (first.isEmpty() && first.emptyFinished()) {
                    ConcurrentLinkedDeque8.Node<Batch> node = first.node();

                    first.markRemoved();

                    if (node != null && evts.unlinkx(node))
                        size.decrementAndGet();

                    assert first.isEmpty();
                }
                else if (polled.isEmpty())
                    break;

                res.addAll(polled);

                if (res.size() == cnt)
                    break;
            }
            else
                break;
        }

        // Update global size.
        tup.totalQueueSize().addAndGet(-res.size());

        return res;
    }

    /**
     * Consistency check, used for testing.
     */
    void consistencyCheck() {
        QueueHolder win = holder;

        Iterator<E> it = iterator();

        int cnt = 0;

        while (it.hasNext()) {
            it.next();

            cnt++;
        }

        int cnt0 = 0;

        for (Batch batch : win.batchQueue())
            cnt0 += batch.size();

        int sz = size();

        assert cnt0 == sz : "Batch size comparison failed [batchCnt=" + cnt0 + ", size=" + sz + ']';
        assert cnt == sz : "Queue size comparison failed [iterCnt=" + cnt + ", size=" + sz + ']';
        assert win.batchQueue().size() == win.batchQueueSize().get();
    }

    /**
     * Window structure.
     */
    private class QueueHolder extends GridTuple3<ConcurrentLinkedDeque8<Batch>, AtomicInteger, AtomicInteger> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public QueueHolder() {
            // No-op.
        }

        /**
         * @param batchQueue Batch queue.
         * @param batchQueueSize Batch queue size counter.
         * @param globalSize Global size counter.
         */
        private QueueHolder(ConcurrentLinkedDeque8<Batch> batchQueue,
            AtomicInteger batchQueueSize, @Nullable AtomicInteger globalSize) {
            super(batchQueue, batchQueueSize, globalSize);

            assert batchQueue.size() == 1;
            assert batchQueueSize.get() == 1;
        }

        /**
         * @return Events queue.
         */
        @SuppressWarnings("ConstantConditions")
        public ConcurrentLinkedDeque8<Batch> batchQueue() {
            return get1();
        }

        /**
         * @return Batch queue size.
         */
        @SuppressWarnings("ConstantConditions")
        public AtomicInteger batchQueueSize() {
            return get2();
        }

        /**
         * @return Global queue size.
         */
        @SuppressWarnings("ConstantConditions")
        public AtomicInteger totalQueueSize() {
            return get3();
        }
    }

    /**
     * Batch.
     */
    private class Batch extends ReentrantReadWriteLock implements Iterable<E> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Batch events. */
        private ConcurrentLinkedDeque8<E> evts;

        /** Capacity. */
        private AtomicInteger cap;

        /** Finished. */
        private volatile boolean finished;

        /** Queue node. */
        @GridToStringExclude
        private ConcurrentLinkedDeque8.Node<Batch> qNode;

        /** Node removed flag. */
        private volatile boolean rmvd;

        /**
         * @param batchSize Batch size.
         */
        private Batch(int batchSize) {
            cap = new AtomicInteger(batchSize);

            evts = new ConcurrentLinkedDeque8<>();
        }

        /**
         * @return {@code True} if batch is removed.
         */
        public boolean removed() {
            return rmvd;
        }

        /**
         * Marks batch as removed.
         */
        public void markRemoved() {
            rmvd = true;
        }

        /**
         * Adds event to batch.
         *
         * @param evt Event to add.
         * @return {@code True} if event was added, {@code false} if batch is full.
         */
        public boolean add(E evt) {
            readLock().lock();

            try {
                if (finished)
                    return false;

                while (true) {
                    int size = cap.get();

                    if (size > 0) {
                        if (cap.compareAndSet(size, size - 1)) {
                            evts.add(evt);

                            // Will go through write lock and finish batch.
                            if (size == 1)
                                finished = true;

                            return true;
                        }
                    }
                    else
                        return false;
                }
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * @return Queue node.
         */
        public ConcurrentLinkedDeque8.Node<Batch> node() {
            return qNode;
        }

        /**
         * @param qNode Queue node.
         */
        public void node(ConcurrentLinkedDeque8.Node<Batch> qNode) {
            this.qNode = qNode;
        }

        /**
         * Waits for latch count down after last event was added.
         *
         * @throws org.apache.ignite.IgniteInterruptedException If wait was interrupted.
         */
        public void finish() throws IgniteInterruptedException {
            writeLock().lock();

            try {
                // Safety.
                assert cap.get() == 0;
                assert finished;
            }
            finally {
                writeLock().unlock();
            }
        }

        /**
         * @return {@code True} if batch is finished and no more events will be added to it.
         */
        public boolean finished() {
            readLock().lock();

            try {
                return finished;
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * Gets batch size.
         *
         * @return Batch size.
         */
        public int size() {
            readLock().lock();

            try {
                return evts == null ? 0 : evts.sizex();
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * @return {@code True} if batch is empty.
         */
        public boolean isEmpty() {
            readLock().lock();

            try {
                return evts == null || evts.isEmpty();
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * Checks if batch is empty and finished inside write lock. This will ensure that no more entries will
         * be added to batch and it can be safely unlinked from the queue.
         *
         * @return {@code True} if batch is empty and finished.
         */
        public boolean emptyFinished() {
            writeLock().lock();

            try {
                return finished && (evts == null || evts.isEmpty());
            }
            finally {
                writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public ConcurrentLinkedDeque8.IteratorEx<E> iterator() {
            readLock().lock();

            try {
                if (evts != null)
                    return (ConcurrentLinkedDeque8.IteratorEx<E>)evts.iterator();

                return new ConcurrentLinkedDeque8.IteratorEx<E>() {
                    @Override public boolean removex() {
                        throw new NoSuchElementException();
                    }

                    @Override public boolean hasNext() {
                        return false;
                    }

                    @Override public E next() {
                        throw new NoSuchElementException();
                    }

                    @Override public void remove() {
                        throw new NoSuchElementException();
                    }
                };
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * Polls up to {@code cnt} objects from batch in concurrent fashion.
         *
         * @param cnt Number of objects to poll.
         * @return Collection of polled elements or empty collection if nothing to poll.
         */
        public Collection<E> pollNonBatch(int cnt) {
            readLock().lock();

            try {
                if (evts == null)
                    return Collections.emptyList();

                Collection<E> res = new ArrayList<>(cnt);

                for (int i = 0; i < cnt; i++) {
                    E evt = evts.poll();

                    if (evt != null)
                        res.add(evt);
                    else
                        return res;
                }

                return res;
            }
            finally {
                readLock().unlock();
            }
        }

        /**
         * Shrinks this batch. No events can be polled from it after this method.
         *
         * @return Collection of events contained in batch before shrink (empty collection in
         *         case no events were present).
         */
        public Collection<E> shrink() {
            writeLock().lock();

            try {
                if (evts == null)
                    return Collections.emptyList();

                // Since iterator can concurrently delete elements, we must poll here.
                Collection<E> res = new ArrayList<>(evts.sizex());

                E o;

                while ((o = evts.poll()) != null)
                    res.add(o);

                // Nothing cal be polled after shrink.
                evts = null;

                return res;
            }
            finally {
                writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            ConcurrentLinkedDeque8<E> evts0 = evts;

            return S.toString(Batch.class, this, "evtQueueSize", evts0 == null ? 0 : evts0.sizex());
        }
    }
}
