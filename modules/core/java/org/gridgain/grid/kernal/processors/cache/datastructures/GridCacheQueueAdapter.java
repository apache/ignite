/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Common code for {@link GridCacheQueue} implementation.
 */
public abstract class GridCacheQueueAdapter<T> extends AbstractCollection<T> implements GridCacheQueue<T> {
    /** Value returned by closure updating queue header indicating that queue was removed. */
    protected static final long QUEUE_REMOVED_IDX = Long.MIN_VALUE;

    /** */
    protected static final int MAX_UPDATE_RETRIES = 100;

    /** Logger. */
    protected final GridLogger log;

    /** Cache context. */
    protected final GridCacheContext<?, ?> cctx;

    /** Cache. */
    protected final GridCacheAdapter cache;

    /** Queue name. */
    protected final String queueName;

    /** Queue header key. */
    protected final GridCacheQueueKey queueKey;

    /** Queue UUID. */
    protected final GridUuid uuid;

    /** Queue capacity. */
    private final int cap;

    /** Collocation flag. */
    private final boolean collocated;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** Read blocking operations semaphore. */
    @GridToStringExclude
    private final Semaphore readSem;

    /** Write blocking operations semaphore. */
    @GridToStringExclude
    private final Semaphore writeSem;

    /**
     * @param queueName Queue name.
     * @param hdr Queue hdr.
     * @param cctx Cache context.
     */
    @SuppressWarnings("unchecked")
    protected GridCacheQueueAdapter(String queueName, GridCacheQueueHeader hdr, GridCacheContext<?, ?> cctx) {
        this.cctx = cctx;
        this.queueName = queueName;
        uuid = hdr.uuid();
        cap = hdr.capacity();
        collocated = hdr.collocated();
        queueKey = new GridCacheQueueKey(queueName);
        cache = cctx.cache();

        log = cctx.logger(getClass());

        readSem = new Semaphore(hdr.size(), true);

        writeSem = bounded() ? new Semaphore(hdr.capacity() - hdr.size(), true) : null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return queueName;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T item) {
        A.notNull(item, "item");

        return offer(item);
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return collocated;
    }

    /** {@inheritDoc} */
    @Override public int capacity() throws GridException {
        return cap;
    }

    /** {@inheritDoc} */
    @Override public boolean bounded() {
        return cap < Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            GridCacheQueueHeader hdr = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(hdr);

            return hdr.size();
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T peek() throws GridRuntimeException {
        try {
            GridCacheQueueHeader hdr = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(hdr);

            if (hdr.empty())
                return null;

            return (T)cache.get(itemKey(hdr.head()));
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T remove() {
        T res = poll();

        if (res == null)
            throw new NoSuchElementException();

        return res;
    }

    /** {@inheritDoc} */
    @Override public T element() {
        T el = peek();

        if (el == null)
            throw new NoSuchElementException();

        return el;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<T> iterator() {
        try {
            GridCacheQueueHeader hdr = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(hdr);

            return new QueueIterator(hdr);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(T item) throws GridRuntimeException {
        A.notNull(item, "item");

        if (!bounded()) {
            boolean offer = offer(item);

            assert offer;

            return;
        }

        while (true) {
            try {
                writeSem.acquire();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue put interrupted.", e);
            }

            if (offer(item))
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item, long timeout, TimeUnit unit) throws GridRuntimeException {
        A.notNull(item, "item");
        A.ensure(timeout >= 0, "Timeout cannot be negative: " + timeout);

        if (!bounded()) {
            boolean offer = offer(item);

            assert offer;

            return true;
        }

        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (U.currentTimeMillis() < end) {
            boolean retVal = false;

            try {
                if (writeSem.tryAcquire(end - U.currentTimeMillis(), MILLISECONDS))
                    retVal = offer(item);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue put interrupted.", e);
            }

            if (retVal)
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() throws GridRuntimeException {
        while (true) {
            try {
                readSem.acquire();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue take interrupted.", e);
            }

            T e = poll();

            if (e != null)
                return e;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(long timeout, TimeUnit unit) throws GridRuntimeException {
        A.ensure(timeout >= 0, "Timeout cannot be negative: " + timeout);

        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (U.currentTimeMillis() < end) {
            T retVal = null;

            try {
                if (readSem.tryAcquire(end - U.currentTimeMillis(), MILLISECONDS))
                    retVal = poll();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue poll interrupted.", e);
            }

            if (retVal != null)
                return retVal;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        if (!bounded())
            return Integer.MAX_VALUE;

        int remaining = cap - size();

        return remaining > 0 ? remaining : 0;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void clear(int batchSize) throws GridRuntimeException {
        A.ensure(batchSize >= 0, "Batch size cannot be negative: " + batchSize);

        try {
            GridBiTuple<Long, Long> t = (GridBiTuple<Long, Long>)cache.transformCompute(queueKey,
                new ClearClosure(uuid));

            if (t == null)
                return;

            checkRemoved(t.get1());

            removeKeys(cache, uuid, queueName, collocated(), t.get1(), t.get2(), batchSize);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c, int maxElements) {
        int max = Math.min(maxElements, size());

        for (int i = 0; i < max; i++) {
            T el = poll();

            if (el == null)
                return i;

            c.add(el);
        }

        return max;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /**
     * @param cache Cache.
     * @param uuid Queue UUID.
     * @param queueName Queue name.
     * @param collocated Collocation flag.
     * @param startIdx Start key index.
     * @param endIdx End key index.
     * @param batchSize Batch size.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    static void removeKeys(GridCacheProjection cache, GridUuid uuid, String queueName, boolean collocated,
        long startIdx, long endIdx, int batchSize) throws GridException {
        Collection<GridCacheQueueItemKey> keys = new ArrayList<>(batchSize > 0 ? batchSize : 10);

        for (long idx = startIdx; idx < endIdx; idx++) {
            keys.add(itemKey(uuid, queueName, collocated, idx));

            if (batchSize > 0 && keys.size() == batchSize) {
                cache.removeAll(keys);

                keys.clear();
            }
        }

        if (!keys.isEmpty())
            cache.removeAll(keys);
    }

    /**
     * Checks result of closure modifying queue header, throws {@link GridCacheDataStructureRemovedRuntimeException}
     * if queue was removed.
     *
     * @param idx Result of closure execution.
     */
    protected final void checkRemoved(Long idx) {
        if (idx == QUEUE_REMOVED_IDX)
            onRemoved();
    }

    /**
     * Checks queue state, throws {@link GridCacheDataStructureRemovedRuntimeException} if queue was removed.
     *
     * @param hdr Queue hdr.
     */
    protected final void checkRemoved(@Nullable GridCacheQueueHeader hdr) {
        if (queueRemoved(hdr, uuid))
            onRemoved();
    }

    /**
     * Marks queue as removed and throws {@link GridCacheDataStructureRemovedRuntimeException}.
     */
    private void onRemoved() {
        rmvd = true;

        // Free all blocked resources.
        if (bounded()) {
            writeSem.drainPermits();
            writeSem.release(Integer.MAX_VALUE);
        }

        readSem.drainPermits();
        readSem.release(Integer.MAX_VALUE);

        throw new GridCacheDataStructureRemovedRuntimeException("Queue has been removed from cache: " + this);
    }

    /**
     * @param hdr Queue header.
     */
    void onHeaderChanged(GridCacheQueueHeader hdr) {
        if (!hdr.empty()) {
            readSem.drainPermits();
            readSem.release(hdr.size());
        }

        if (bounded()) {
            writeSem.drainPermits();

            if (!hdr.full())
                writeSem.release(hdr.capacity() - hdr.size());
        }
    }

    /**
     * @return Queue UUID.
     */
    GridUuid queueId() {
        return uuid;
    }

    /**
     * Removes item with given index from queue.
     *
     * @param rmvIdx Index of item to be removed.
     * @throws GridException If failed.
     */
    protected abstract void removeItem(long rmvIdx) throws GridException;


    /**
     * @param idx Item index.
     * @return Item key.
     */
    protected GridCacheQueueItemKey itemKey(Long idx) {
        return itemKey(uuid, queueName, collocated(), idx);
    }

    /**
     * @param uuid Queue UUID.
     * @param queueName Queue name.
     * @param collocated Collocation flag.
     * @param idx Item index.
     * @return Item key.
     */
    private static GridCacheQueueItemKey itemKey(GridUuid uuid, String queueName, boolean collocated, long idx) {
        return collocated ? new CollocatedItemKey(uuid, queueName, idx) :
            new GridCacheQueueItemKey(uuid, queueName, idx);
    }

    /**
     * @param hdr Queue header.
     * @param uuid Expected queue UUID.
     * @return {@code True} if queue was removed.
     */
    private static boolean queueRemoved(@Nullable GridCacheQueueHeader hdr, GridUuid uuid) {
        return hdr == null || !uuid.equals(hdr.uuid());
    }

    /**
     */
    private class QueueIterator implements Iterator<T> {
        /** */
        private T next;

        /** */
        private T cur;

        /** */
        private long curIdx;

        /** */
        private long idx;

        /** */
        private long endIdx;

        /** */
        private Set<Long> rmvIdxs;

        /**
         * @param hdr Queue header.
         * @throws GridException If failed.
         */
        @SuppressWarnings("unchecked")
        private QueueIterator(GridCacheQueueHeader hdr) throws GridException {
            idx = hdr.head();
            endIdx = hdr.tail();
            rmvIdxs = hdr.removedIndexes();

            assert !F.contains(rmvIdxs, idx) : idx;

            if (idx < endIdx)
                next = (T)cache.get(itemKey(idx));
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public T next() {
            if (next == null)
                throw new NoSuchElementException();

            try {
                cur = next;
                curIdx = idx;

                idx++;

                if (rmvIdxs != null) {
                    while (F.contains(rmvIdxs, idx) && idx < endIdx)
                        idx++;
                }

                next = idx < endIdx ? (T)cache.get(itemKey(idx)) : null;

                return cur;
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (cur == null)
                throw new IllegalStateException();

            try {
                removeItem(curIdx);

                cur = null;
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }

    /**
     * Item key for collocated queue.
     */
    private static class CollocatedItemKey extends GridCacheQueueItemKey {
        /**
         * Required by {@link Externalizable}.
         */
        public CollocatedItemKey() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         * @param queueName Queue name.
         * @param idx Item index.
         */
        private CollocatedItemKey(GridUuid uuid, String queueName, long idx) {
            super(uuid, queueName, idx);
        }

        /**
         * @return Item affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return queueId();
        }
    }

    /**
     */
    protected static class ClearClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader,
        GridBiTuple<Long, Long>>,
        Externalizable {
        /** */
        private GridUuid uuid;

        /**
         * Required by {@link Externalizable}.
         */
        public ClearClosure() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         */
        public ClearClosure(GridUuid uuid) {
            this.uuid = uuid;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Long, Long> compute(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid))
                return new GridBiTuple<>(QUEUE_REMOVED_IDX, QUEUE_REMOVED_IDX);

            return hdr.empty() ? null : new GridBiTuple<>(hdr.head(), hdr.tail());
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid) || hdr.empty())
                return hdr;

            return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(), hdr.tail(),
                hdr.tail(), null);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, uuid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uuid = U.readGridUuid(in);
        }
    }

    /**
     */
    protected static class PollClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader, Long>,
        Externalizable {
        /** */
        private GridUuid uuid;

        /**
         * Required by {@link Externalizable}.
         */
        public PollClosure() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         */
        public PollClosure(GridUuid uuid) {
            this.uuid = uuid;
        }

        /** {@inheritDoc} */
        @Override public Long compute(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid))
                return QUEUE_REMOVED_IDX;

            if (hdr.empty())
                return null;

            Set<Long> rmvIdx = hdr.removedIndexes();

            if (rmvIdx == null)
                return hdr.head();

            long next = hdr.head();

            do {
                if (!rmvIdx.contains(next))
                    return next;

                next++;
            } while (next != hdr.tail());

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid) || hdr.empty())
                return hdr;

            Set<Long> removedIdx = hdr.removedIndexes();

            if (removedIdx == null)
                return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                    hdr.head() + 1, hdr.tail(), removedIdx);

            long next = hdr.head() + 1;

            do {
                if (!removedIdx.remove(next))
                    return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                        next + 1, hdr.tail(), removedIdx.isEmpty() ? null : new HashSet<>(removedIdx));

                next++;
            } while (next != hdr.tail());

            return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                next, hdr.tail(), removedIdx.isEmpty() ? null : new HashSet<>(removedIdx));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, uuid);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uuid = U.readGridUuid(in);
        }
    }

    /**
     */
    protected static class AddClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader, Long>,
        Externalizable {
        /** */
        private GridUuid uuid;

        /** */
        private int size;

        /**
         * Required by {@link Externalizable}.
         */
        public AddClosure() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         * @param size Number of elements to add.
         */
        public AddClosure(GridUuid uuid, int size) {
            this.uuid = uuid;
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public Long compute(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid))
                return QUEUE_REMOVED_IDX;

            if (!spaceAvailable(hdr, size))
                return null;

            return hdr.tail();
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid) || !spaceAvailable(hdr, size))
                return hdr;

            return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(), hdr.head(),
                hdr.tail() + size, hdr.removedIndexes());
        }

        /**
         * @param hdr Queue header.
         * @param size Number of elements to add.
         * @return {@code True} if new elements can be added.
         */
        private boolean spaceAvailable(GridCacheQueueHeader hdr, int size) {
            return !hdr.bounded() || (hdr.size() + size) <= hdr.capacity();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, uuid);
            out.writeInt(size);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uuid = U.readGridUuid(in);
            size = in.readInt();
        }
    }

    /**
     */
    protected static class RemoveClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader, Long>,
        Externalizable {
        /** */
        private GridUuid uuid;

        /** */
        private Long idx;

        /**
         * Required by {@link Externalizable}.
         */
        public RemoveClosure() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         * @param idx Index of item to be removed.
         */
        public RemoveClosure(GridUuid uuid, Long idx) {
            this.uuid = uuid;
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public Long compute(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid))
                return QUEUE_REMOVED_IDX;

            if (hdr.empty() || idx < hdr.head() || F.contains(hdr.removedIndexes(), idx))
                return null;

            return idx;
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader hdr) {
            if (queueRemoved(hdr, uuid) || hdr.empty())
                return hdr;

            assert idx < hdr.tail();

            if (idx < hdr.head())
                return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                    hdr.head(), hdr.tail(), hdr.removedIndexes());

            if (idx == hdr.head()) {
                Set<Long> rmvIdxs = hdr.removedIndexes();

                long head = hdr.head() + 1;

                if (!F.contains(rmvIdxs, head))
                    return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                        head, hdr.tail(), hdr.removedIndexes());

                while (rmvIdxs.remove(head))
                    head++;

                return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                    head, hdr.tail(), new HashSet<>(rmvIdxs));
            }

            Set<Long> rmvIdxs = hdr.removedIndexes();

            if (rmvIdxs == null) {
                rmvIdxs = new HashSet<>();

                rmvIdxs.add(idx);
            }
            else if (!rmvIdxs.contains(idx)) {
                rmvIdxs = new HashSet<>(rmvIdxs);

                rmvIdxs.add(idx);
            }

            return new GridCacheQueueHeader(hdr.uuid(), hdr.capacity(), hdr.collocated(),
                hdr.head(), hdr.tail(), rmvIdxs);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, uuid);
            out.writeLong(idx);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uuid = U.readGridUuid(in);
            idx = in.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueueAdapter that = (GridCacheQueueAdapter) o;

        return uuid.equals(that.uuid);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return uuid.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueAdapter.class, this);
    }
}
