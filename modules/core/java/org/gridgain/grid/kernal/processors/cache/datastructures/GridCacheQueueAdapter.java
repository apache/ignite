/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Common code for {@link GridCacheQueue} implementation.
 */
public abstract class GridCacheQueueAdapter<T> extends AbstractCollection<T> implements GridCacheQueueEx<T> {
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
    @Override public GridCacheInternalKey key() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onHeaderChanged(GridCacheQueueHeader hdr) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onInvalid(@Nullable Exception err) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void put(T item) throws GridRuntimeException {
        add(item);
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
    @Override public boolean bounded() throws GridException {
        return cap < Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            GridCacheQueueHeader2 header = (GridCacheQueueHeader2)cache.get(queueKey);

            int size = (int)(header.tail() - header.head());

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
            GridCacheQueueHeader2 header = (GridCacheQueueHeader2)cache.get(queueKey);

            if (header.head() == header.tail())
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
            GridCacheQueueHeader2 header = (GridCacheQueueHeader2)cache.get(queueKey);

            return new QueueIterator(header);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     */
    private class QueueIterator implements Iterator<T> {
        /** */
        private T cur;

        /** */
        private long idx;

        /** */
        private long endIdx;

        /**
         * @param header Queue header.
         * @throws GridException If failed.
         */
        @SuppressWarnings("unchecked")
        private QueueIterator(GridCacheQueueHeader2 header) throws GridException {
            idx = header.head();
            endIdx = header.tail();

            if (idx < endIdx)
                cur = (T)cache.get(new ItemKey(uuid, idx, collocated()));
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return cur != null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public T next() {
            if (cur == null)
                throw new NoSuchElementException();

            try {
                T res = cur;

                idx++;

                cur = idx < endIdx ? (T)cache.get(new ItemKey(uuid, idx, collocated())) : null;

                return res;
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (cur == null)
                throw new IllegalStateException();

            // TODO
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
    protected static class PollClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader2, Long>,
        Externalizable {
        /**
         * Required by {@link Externalizable}.
         */
        public PollClosure() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Long compute(GridCacheQueueHeader2 header) {
            if (header.tail() == header.head())
                return null;

            return header.head();
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader2 apply(GridCacheQueueHeader2 header) {
            if (header.tail() == header.head())
                return header;

            return new GridCacheQueueHeader2(header.uuid(), header.capacity(), header.head() + 1, header.tail());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }

    /**
     */
    protected static class AddClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader2, Long>,
        Externalizable {
        /** */
        private int size;

        /**
         * Required by {@link Externalizable}.
         */
        public AddClosure() {
            // No-op.
        }

        /**
         * @param size Number of elements to add.
         */
        public AddClosure(int size) {
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public Long compute(GridCacheQueueHeader2 header) {
            if (!spaceAvailable(header, size))
                return null;

            return header.tail();
        }

        /** {@inheritDoc} */
        @Override public GridCacheQueueHeader2 apply(GridCacheQueueHeader2 header) {
            if (!spaceAvailable(header, size))
                return header;

            return new GridCacheQueueHeader2(header.uuid(), header.capacity(), header.head(), header.tail() + size);
        }

        /**
         * @param header Queue header.
         * @param size Number of elements to add.
         * @return {@code True} if new elements can be added.
         */
        private boolean spaceAvailable(GridCacheQueueHeader2 header, int size) {
            return !header.bounded() || (header.tail() - header.head() + size) <= header.capacity();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(size);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            size = in.readInt();
        }
    }
}
