/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Common code for {@link GridCacheQueue} implementation.
 */
public abstract class GridCacheQueueAdapter<T> extends AbstractCollection<T> implements GridCacheQueueEx<T> {
    /** */
    protected final String queueName;

    /** */
    protected GridCacheAdapter cache;

    /** */
    protected GridCacheQueueKey queueKey;

    /** */
    protected GridUuid uuid;

    /**
     * @param queueName Queue name.
     * @param uuid Queue UUID.
     * @param cctx Cache context.
     */
    protected GridCacheQueueAdapter(String queueName, GridUuid uuid, GridCacheContext<?, ?> cctx) {
        this.queueName = queueName;
        this.uuid = uuid;
        queueKey = new GridCacheQueueKey(queueName);
        cache = cctx.cache();
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

            return (T)cache.get(new ItemKey(uuid, header.tail()));
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

    /**
     */
    protected static class ItemKey implements Externalizable {
        /** */
        private GridUuid uuid;

        /** */
        private long idx;

        /**
         * Required by {@link Externalizable}.
         */
        public ItemKey() {
            // No-op.
        }

        /**
         * @param uuid Queue UUID.
         * @param idx Item index.
         */
        protected ItemKey(GridUuid uuid, long idx) {
            this.uuid = uuid;
            this.idx = idx;
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
        @Override public void writeExternal(ObjectOutput out) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
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

            return new GridCacheQueueHeader2(header.uuid(), header.head() + 1, header.tail());
        }
    }

    /**
     */
    protected static class AddClosure implements GridCacheTransformComputeClosure<GridCacheQueueHeader2, Long>,
        Externalizable {
        /**
         * Required by {@link Externalizable}.
         */
        public AddClosure() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }

        @Override public Long compute(GridCacheQueueHeader2 header) {
            return header.tail();
        }

        @Override public GridCacheQueueHeader2 apply(GridCacheQueueHeader2 header) {
            return new GridCacheQueueHeader2(header.uuid(), header.head(), header.tail() + 1);
        }
    }
}
