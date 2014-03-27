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
    /** Value returned by queue update closure indicating that queue was removed. */
    protected static final long QUEUE_REMOVED_IDX = Long.MIN_VALUE;

    /** */
    protected final GridLogger log;

    /** */
    protected final String queueName;

    /** */
    protected final GridCacheAdapter cache;

    /** */
    protected final GridCacheQueueKey queueKey;

    /** */
    protected final GridUuid uuid;

    /** */
    private final int cap;

    /** */
    private final boolean collocated;

    /** */
    private volatile boolean rmvd;

    /**
     * @param queueName Queue name.
     * @param uuid Queue UUID.
     * @param cap Capacity.
     * @param collocated Collocation flag.
     * @param cctx Cache context.
     */
    protected GridCacheQueueAdapter(String queueName, GridUuid uuid, int cap, boolean collocated,
        GridCacheContext<?, ?> cctx) {
        this.queueName = queueName;
        this.uuid = uuid;
        this.cap = cap;
        this.collocated = collocated;
        queueKey = new GridCacheQueueKey(queueName);
        cache = cctx.cache();

        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return queueName;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T item) {
        return offer(item);
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() throws GridException {
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
            GridCacheQueueHeader header = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(header);

            int rmvSize = F.isEmpty(header.removedIndexes()) ? 0 : header.removedIndexes().size();

            int size = (int)(header.tail() - header.head() - rmvSize);

            assert size >= 0 : size;

            return size;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T peek() throws GridRuntimeException {
        try {
            GridCacheQueueHeader header = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(header);

            if (header.empty())
                return null;

            return (T)cache.get(new ItemKey(uuid, header.head(), collocated()));
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
            GridCacheQueueHeader header = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(header);

            return new QueueIterator(header);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(T item) throws GridRuntimeException {
        do {
            if (offer(item))
                return;

            if (Thread.interrupted())
                throw new GridRuntimeException("Queue put interrupted.");
        } while (true);
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item, long timeout, TimeUnit unit) throws GridRuntimeException {
        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (U.currentTimeMillis() < end) {
            if (offer(item))
                return true;

            if (Thread.interrupted())
                throw new GridRuntimeException("Queue offer interrupted.");
        }

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() throws GridRuntimeException {
        do {
            T e = poll();

            if (e != null)
                return e;

            if (Thread.interrupted())
                throw new GridRuntimeException("Queue take interrupted.");
        } while (true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(long timeout, TimeUnit unit) throws GridRuntimeException {
        long end = U.currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (U.currentTimeMillis() < end) {
            T e = poll();

            if (e != null)
                return e;

            if (Thread.interrupted())
                throw new GridRuntimeException("Queue poll interrupted.");
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
        try {
            GridBiTuple<Long, Long> t = (GridBiTuple<Long, Long>)cache.transformCompute(queueKey,
                new ClearClosure(uuid));

            if (t == null)
                return;

            checkRemoved(t.get1());

            removeKeys(cache, uuid, collocated(), t.get1(), t.get2(), batchSize);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
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

    /**
     * @param cache Cache.
     * @param uuid Queue UUID.
     * @param collocated Collocation flag.
     * @param startIdx Start key index.
     * @param endIdx End key index.
     * @param batchSize Batch size.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    static void removeKeys(GridCacheProjection cache, GridUuid uuid, boolean collocated, long startIdx, long endIdx,
        int batchSize) throws GridException {
        Collection<ItemKey> keys = new ArrayList<>(batchSize != 0 ? batchSize : 10);

        for (long idx = startIdx; idx < endIdx; idx++) {
            keys.add(new ItemKey(uuid, idx, collocated));

            if (batchSize > 0 && keys.size() == batchSize) {
                cache.removeAll(keys);

                keys.clear();
            }
        }

        if (!keys.isEmpty())
            cache.removeAll(keys);
    }

    /**
     * Checks result of closure modifying queue, throws {@link GridCacheDataStructureRemovedRuntimeException}
     * if queue was removed.
     *
     * @param idx Result of closure execution.
     */
    protected final void checkRemoved(Long idx) {
        if (idx == QUEUE_REMOVED_IDX) {
            rmvd = true;

            throw new GridCacheDataStructureRemovedRuntimeException("Queue has been removed from cache: " + this);
        }
    }

    /**
     * Checks queue state, throws {@link GridCacheDataStructureRemovedRuntimeException}
     * if queue was removed.
     *
     * @param header Queue header.
     */
    protected final void checkRemoved(@Nullable GridCacheQueueHeader header) {
        if (queueRemoved(header, uuid)) {
            rmvd = true;

            throw new GridCacheDataStructureRemovedRuntimeException("Queue has been removed from cache: " + this);
        }
    }

    /**
     * Removes item with given index from queue.
     *
     * @param rmvIdx Index of item to be removed.
     * @throws GridException If failed.
     */
    protected abstract void removeItem(long rmvIdx) throws GridException;

    /**
     * @param header Queue header.
     * @param uuid Expected queue UUID.
     * @return {@code True} if queue was removed.
     */
    private static boolean queueRemoved(@Nullable GridCacheQueueHeader header, GridUuid uuid) {
        return header == null || !uuid.equals(header.uuid());
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
         * @param header Queue header.
         * @throws GridException If failed.
         */
        @SuppressWarnings("unchecked")
        private QueueIterator(GridCacheQueueHeader header) throws GridException {
            idx = header.head();
            endIdx = header.tail();
            rmvIdxs = header.removedIndexes();

            assert !F.contains(rmvIdxs, idx) : idx;

            if (idx < endIdx)
                next = (T)cache.get(new ItemKey(uuid, idx, collocated()));
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

                next = idx < endIdx ? (T)cache.get(new ItemKey(uuid, idx, collocated())) : null;

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
     */
    protected static class ItemKey implements Externalizable {
        /** */
        private GridUuid uuid;

        /** */
        private long idx;

        /** */
        private boolean collocated;

        /**
         * Required by {@link Externalizable}.
         */
        public ItemKey() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         * @param idx Item index.
         * @param collocated Collocation flag.
         */
        protected ItemKey(GridUuid uuid, long idx, boolean collocated) {
            this.uuid = uuid;
            this.idx = idx;
            this.collocated = collocated;
        }

        /**
         * @return Item affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return collocated ? uuid : idx;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, uuid);
            out.writeLong(idx);
            out.writeBoolean(collocated);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            uuid = U.readGridUuid(in);
            idx = in.readLong();
            collocated = in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ItemKey itemKey = (ItemKey)o;

            return idx == itemKey.idx && uuid.equals(itemKey.uuid);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = uuid.hashCode();

            result = 31 * result + (int) (idx ^ (idx >>> 32));

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ItemKey.class, this);
        }
    }

    /**
     */
    protected static class ClearClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader, GridBiTuple<Long, Long>>,
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
        @Override public GridBiTuple<Long, Long> compute(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid))
                return new GridBiTuple<>(QUEUE_REMOVED_IDX, QUEUE_REMOVED_IDX);

            if (header.empty())
                return null;

            return header.empty() ? null : new GridBiTuple<>(header.head(), header.tail());
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid) || header.empty())
                return header;

            return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(), header.tail(),
                header.tail(), null);
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
        @Override public Long compute(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid))
                return QUEUE_REMOVED_IDX;

            if (header.empty())
                return null;

            Set<Long> rmvIdx = header.removedIndexes();

            if (rmvIdx == null)
                return header.head();

            long next = header.head();

            do {
                if (!rmvIdx.contains(next))
                    return next;

                next++;
            } while (next != header.tail());

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid) || header.empty())
                return header;

            Set<Long> removedIdx = header.removedIndexes();

            if (removedIdx == null)
                return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                    header.head() + 1, header.tail(), removedIdx);

            long next = header.head() + 1;

            do {
                if (!removedIdx.remove(next))
                    return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                        next + 1, header.tail(), removedIdx.isEmpty() ? null : new HashSet<>(removedIdx));

                next++;
            } while (next != header.tail());

            return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                next, header.tail(), removedIdx.isEmpty() ? null : new HashSet<>(removedIdx));
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
        @Override public Long compute(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid))
                return QUEUE_REMOVED_IDX;

            if (!spaceAvailable(header, size))
                return null;

            return header.tail();
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid) || !spaceAvailable(header, size))
                return header;

            return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(), header.head(),
                header.tail() + size, header.removedIndexes());
        }

        /**
         * @param header Queue header.
         * @param size Number of elements to add.
         * @return {@code True} if new elements can be added.
         */
        private boolean spaceAvailable(GridCacheQueueHeader header, int size) {
            return !header.bounded() || (header.tail() - header.head() + size) <= header.capacity();
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
        @Override public Long compute(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid))
                return QUEUE_REMOVED_IDX;

            if (header.empty() || idx < header.head() || F.contains(header.removedIndexes(), idx))
                return null;

            return idx;
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader apply(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid) || header.empty())
                return header;

            assert idx < header.tail();

            if (idx < header.head())
                return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                    header.head(), header.tail(), header.removedIndexes());

            if (idx == header.head()) {
                Set<Long> rmvIdxs = header.removedIndexes();

                long head = header.head() + 1;

                if (!F.contains(rmvIdxs, head))
                    return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                        head, header.tail(), header.removedIndexes());

                while (rmvIdxs.remove(head))
                    head++;

                return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                    head, header.tail(), new HashSet<>(rmvIdxs));
            }

            Set<Long> rmvIdxs = header.removedIndexes();

            if (rmvIdxs == null) {
                rmvIdxs = new HashSet<>();

                rmvIdxs.add(idx);
            }
            else if (!rmvIdxs.contains(idx)) {
                rmvIdxs = new HashSet<>(rmvIdxs);

                rmvIdxs.add(idx);
            }

            return new GridCacheQueueHeader(header.uuid(), header.capacity(), header.collocated(),
                header.head(), header.tail(), rmvIdxs);
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
