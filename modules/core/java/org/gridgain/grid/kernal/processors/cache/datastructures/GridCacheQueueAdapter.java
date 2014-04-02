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
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Common code for {@link GridCacheQueue} implementation.
 */
public abstract class GridCacheQueueAdapter<T> extends AbstractCollection<T> implements GridCacheQueue<T> {
    /** Value returned by closure updating queue header indicating that queue was removed. */
    protected static final long QUEUE_REMOVED_IDX = Long.MIN_VALUE;

    /** */
    protected static final int MAX_UPDATE_RETRIES = 100;

    /** */
    protected final GridLogger log;

    /** */
    protected final GridCacheContext<?, ?> cctx;

    /** */
    protected final GridCacheAdapter cache;

    /** */
    protected final String queueName;

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

    /** */
    private Lock lock = new ReentrantLock();

    /** */
    private Condition addCond = lock.newCondition();

    /** */
    private Condition pollCond = lock.newCondition();

    /** */
    private boolean full;

    /** */
    private boolean empty;

    /**
     * @param queueName Queue name.
     * @param header Queue header.
     * @param cctx Cache context.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    protected GridCacheQueueAdapter(String queueName, GridCacheQueueHeader header, GridCacheContext<?, ?> cctx)
        throws GridException {
        this.cctx = cctx;
        this.queueName = queueName;
        uuid = header.uuid();
        cap = header.capacity();
        collocated = header.collocated();
        queueKey = new GridCacheQueueKey(queueName);
        cache = cctx.cache();

        log = cctx.logger(getClass());

        full = header.full();
        empty = header.empty();

        GridCacheContinuousQueryAdapter qry = (GridCacheContinuousQueryAdapter)cache.queries().createContinuousQuery();

        qry.filter(new QueuePredicate(queueName));

        qry.callback(new GridBiPredicate<UUID, Collection<Map.Entry>>() {
            @Override public boolean apply(UUID uuid, Collection<Map.Entry> entries) {
                for (Map.Entry e : entries) {
                    GridCacheQueueHeader header = (GridCacheQueueHeader)e.getValue();

                    lock.lock();

                    try {
                        if (header == null) {
                            full = false;
                            empty = false;

                            addCond.signalAll();
                            pollCond.signalAll();

                            return false;
                        }
                        else {
                            full = header.full();
                            empty = header.empty();

                            if (!full)
                                addCond.signalAll();

                            if (!empty)
                                pollCond.signalAll();
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

                return true;
            }
        });

        qry.execute(cctx.isLocal() || cctx.isReplicated() ? cctx.grid().forLocal() : null, true);
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
            GridCacheQueueHeader header = (GridCacheQueueHeader)cache.get(queueKey);

            checkRemoved(header);

            return header.size();
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

            return (T)cache.get(itemKey(header.head()));
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
        A.notNull(item, "item");

        if (!bounded()) {
            boolean offer = offer(item);

            assert offer;

            return;
        }

        while (true) {
            lock.lock();

            try {
                while (full)
                    addCond.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue put interrupted.", e);
            }
            finally {
                lock.unlock();
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
            lock.lock();

            try {
                while (full) {
                    if (!addCond.await(end - U.currentTimeMillis(), MILLISECONDS))
                        return false;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue put interrupted.", e);
            }
            finally {
                lock.unlock();
            }

            if (offer(item))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() throws GridRuntimeException {
        while (true) {
            lock.lock();

            try {
                while (empty)
                    pollCond.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue take interrupted.", e);
            }
            finally {
                lock.unlock();
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
            lock.lock();

            try {
                while (empty) {
                    if (!pollCond.await(end - U.currentTimeMillis(), MILLISECONDS))
                        return null;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Queue poll interrupted.", e);
            }
            finally {
                lock.unlock();
            }

            T e = poll();

            if (e != null)
                return e;
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
        if (idx == QUEUE_REMOVED_IDX) {
            rmvd = true;

            throw new GridCacheDataStructureRemovedRuntimeException("Queue has been removed from cache: " + this);
        }
    }

    /**
     * Checks queue state, throws {@link GridCacheDataStructureRemovedRuntimeException} if queue was removed.
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
     * @param header Queue header.
     * @param uuid Expected queue UUID.
     * @return {@code True} if queue was removed.
     */
    private static boolean queueRemoved(@Nullable GridCacheQueueHeader header, GridUuid uuid) {
        return header == null || !uuid.equals(header.uuid());
    }

    /**
     * Queue predicate for continuous query.
     */
    private static class QueuePredicate implements GridBiPredicate, Externalizable {
        /** */
        private String name;

        /**
         * Required by {@link Externalizable}.
         */
        public QueuePredicate() {
            // No-op.
        }

        /**
         * @param name Queue name.
         */
        private QueuePredicate(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof GridCacheQueueKey && ((GridCacheQueueKey)key).queueName().equals(name);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
        }
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
        @Override public GridBiTuple<Long, Long> compute(@Nullable GridCacheQueueHeader header) {
            if (queueRemoved(header, uuid))
                return new GridBiTuple<>(QUEUE_REMOVED_IDX, QUEUE_REMOVED_IDX);

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
            return !header.bounded() || (header.size() + size) <= header.capacity();
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
