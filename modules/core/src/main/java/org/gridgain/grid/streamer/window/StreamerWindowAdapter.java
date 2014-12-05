/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.window;

import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.streamer.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.streamer.index.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Streamer window adapter.
 */
public abstract class StreamerWindowAdapter<E> implements LifecycleAware, StreamerWindow<E>,
    StreamerWindowMBean {
    /** Default window name. */
    private String name = getClass().getSimpleName();

    /** Filter predicate. */
    private IgnitePredicate<Object> filter;

    /** Indexes. */
    private Map<String, StreamerIndexProvider<E, ?, ?>> idxsAsMap;

    /** */
    private StreamerIndexProvider<E, ?, ?>[] idxs;

    /** Lock for updates and snapshot. */
    private final GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

    /** {@inheritDoc} */
    @Override public String getClassName() {
        return U.compact(getClass().getName());
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return size();
    }

    /** {@inheritDoc} */
    @Override public int getEvictionQueueSize() {
        return evictionQueueSize();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return new BoundedIterator(iterator0());
    }

    /**
     * Returns an iterator over a set of elements of type T without check for iteration limit. That is,
     * in case concurrent thread constantly adding new elements to the window we could iterate forever.
     *
     * @return Iterator.
     */
    protected abstract GridStreamerWindowIterator<E> iterator0();

    /** {@inheritDoc} */
    @Override public boolean enqueue(E evt) throws GridException {
        lock.readLock();

        try {
            boolean res = (filter == null || filter.apply(evt));

            if (res) {
                updateIndexes(evt, false);

                if (!enqueue0(evt))
                    updateIndexes(evt, true);
            }

            return res;
        }
        finally {
            lock.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean enqueue(E... evts) throws GridException {
        return enqueueAll(Arrays.asList(evts));
    }

    /** {@inheritDoc} */
    @Override public boolean enqueueAll(Collection<E> evts) throws GridException {
        lock.readLock();

        try {
            boolean ignoreFilter = filter == null || F.isAlwaysTrue(filter);

            boolean res = true;

            for (E evt : evts) {
                if (ignoreFilter || filter.apply(evt)) {
                    updateIndexes(evt, false);

                    boolean added = enqueue0(evt);

                    if (!added)
                        updateIndexes(evt, true);

                    res &= added;
                }
            }

            return res;
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Adds event to window.
     *
     * @param evt Event.
     * @return {@code True} if event added.
     */
    protected abstract boolean enqueue0(E evt);

    /** {@inheritDoc} */
    @Override public E dequeue() throws GridException {
        return F.first(dequeue(1));
    }

    /** {@inheritDoc} */
    @Override public Collection<E> dequeueAll() throws GridException {
        return dequeue(size());
    }

    /** {@inheritDoc} */
    @Override public Collection<E> dequeue(int cnt) throws GridException {
        lock.readLock();

        try {
            Collection<E> evts = dequeue0(cnt);

            if (!evts.isEmpty() && idxs != null) {
                for (E evt : evts)
                    updateIndexes(evt, true);
            }

            return evts;
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Dequeues up to cnt elements from window. If current window size is less than cnt, will dequeue all elements
     * from window.
     *
     * @param cnt Count.
     * @return Dequeued elements.
     */
    protected abstract Collection<E> dequeue0(int cnt);

    /** {@inheritDoc} */
    @Override public E pollEvicted() throws GridException {
        return F.first(pollEvicted(1));
    }

    /** {@inheritDoc} */
    @Override public Collection<E> pollEvictedAll() throws GridException {
        return pollEvicted(evictionQueueSize());
    }

    /** {@inheritDoc} */
    @Override public Collection<E> pollEvicted(int cnt) throws GridException {
        lock.readLock();

        try {
            Collection<E> evts = pollEvicted0(cnt);

            if (!evts.isEmpty() && idxs != null) {
                for (E evt : evts)
                    updateIndexes(evt, true);
            }

            return evts;
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * If window supports eviction, this method will return up to cnt evicted elements.
     *
     * @param cnt Count.
     * @return Evicted elements.
     */
    protected abstract Collection<E> pollEvicted0(int cnt);

    /** {@inheritDoc} */
    @Override public Collection<E> pollEvictedBatch() throws GridException {
        lock.readLock();

        try {
            Collection<E> evts = pollEvictedBatch0();

            if (!evts.isEmpty() && idxs != null) {
                for (E evt : evts)
                    updateIndexes(evt, true);
            }

            return evts;
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * If window supports batch eviction, this method will poll next evicted batch from window. If windows does not
     * support batch eviction but supports eviction, will return collection of single last evicted element. If window
     * does not support eviction, will return empty collection.
     *
     * @return Elements from evicted batch.
     */
    protected abstract Collection<E> pollEvictedBatch0();

    /** {@inheritDoc} */
    @Override public final void start() throws GridException {
        checkConfiguration();

        if (idxs != null) {
            for (StreamerIndexProvider<E, ?, ?> idx : idxs)
                idx.initialize();
        }

        reset();
    }

    /** {@inheritDoc} */
    @Override public final void reset(){
        lock.writeLock();

        try {
            if (idxs != null) {
                for (StreamerIndexProvider<E, ?, ?> idx : idxs)
                    idx.reset();
            }

            reset0();
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Check window configuration.
     *
     * @throws GridException If failed.
     */
    protected abstract void checkConfiguration() throws GridException;

    /**
     * Reset routine.
     */
    protected abstract void reset0();

    /** {@inheritDoc} */
    @Override public void stop() {
        lock.writeLock();

        try {
            stop0();
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Dispose window.
     */
    protected abstract void stop0();

    /** {@inheritDoc} */
    @Override public Collection<E> snapshot(boolean includeEvicted) {
        lock.writeLock();

        try {
            int skip = includeEvicted ? 0 : evictionQueueSize();

            List<E> res = new ArrayList<>(size() - skip);

            Iterator<E> iter = iterator();

            int i = 0;

            while (iter.hasNext()) {
                E next = iter.next();

                if (i++ >= skip)
                    res.add(next);
            }

            return Collections.unmodifiableList(res);
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Sets window name.
     *
     * @param name Window name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets optional event filter.
     *
     * @return Optional event filter.
     */
    @Nullable public IgnitePredicate<Object> getFilter() {
        return filter;
    }

    /**
     * Sets event filter.
     *
     * @param filter Event filter.
     */
    public void setFilter(@Nullable IgnitePredicate<Object> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public <K, V> StreamerIndex<E, K, V> index() {
        return index(null);
    }

    /** {@inheritDoc} */
    @Override public <K, V> StreamerIndex<E, K, V> index(@Nullable String name) {
        if (idxsAsMap != null) {
            StreamerIndexProvider<E, K, V> idx = (StreamerIndexProvider<E, K, V>)idxsAsMap.get(name);

            if (idx == null)
                throw new IllegalArgumentException("Streamer index is not configured: " + name);

            return idx.index();
        }

        throw new IllegalArgumentException("Streamer index is not configured: " + name);
    }

    /** {@inheritDoc} */
    @Override public Collection<StreamerIndex<E, ?, ?>> indexes() {
        if (idxs != null) {
            Collection<StreamerIndex<E, ?, ?>> res = new ArrayList<>(idxs.length);

            for (StreamerIndexProvider<E, ?, ?> idx : idxs)
                res.add(idx.index());

            return res;
        }
        else
            return Collections.emptyList();
    }

    /**
     * Get array of index providers.
     *
     * @return Index providers.
     */
    public StreamerIndexProvider<E, ?, ?>[] indexProviders() {
        return idxs;
    }

    /**
     * Set indexes.
     *
     * @param idxs Indexes.
     * @throws IllegalArgumentException If some index names are not unique.
     */
    @SuppressWarnings("unchecked")
    public void setIndexes(StreamerIndexProvider<E, ?, ?>... idxs) throws IllegalArgumentException {
        A.ensure(!F.isEmpty(idxs), "!F.isEmpty(idxs)");

        idxsAsMap = new HashMap<>(idxs.length, 1.0f);
        this.idxs = new StreamerIndexProvider[idxs.length];

        int i = 0;

        for (StreamerIndexProvider<E, ?, ?> idx : idxs) {
            StreamerIndexProvider<E, ?, ?> old = idxsAsMap.put(idx.getName(), idx);

            if (old != null)
                throw new IllegalArgumentException("Index name is not unique [idx1=" + old + ", idx2=" + idx + ']');

            this.idxs[i++] = idx;
        }
    }

    /** {@inheritDoc} */
    @Override public void clearEvicted() throws GridException {
        pollEvictedAll();
    }

    /**
     * Update indexes.
     *
     * @param evt Event.
     * @param rmv Remove flag.
     * @throws GridException If index update failed.
     */
    protected void updateIndexes(E evt, boolean rmv) throws GridException {
        if (idxs != null) {
            StreamerIndexUpdateSync sync = new StreamerIndexUpdateSync();

            boolean rollback = true;

            try {
                for (StreamerIndexProvider<E, ?, ?> idx : idxs) {
                    if (rmv)
                        idx.remove(sync, evt);
                    else
                        idx.add(sync, evt);
                }

                rollback = false;
            }
            finally {
                for (StreamerIndexProvider<E, ?, ?> idx : idxs)
                    idx.endUpdate(sync, evt, rollback, rmv);

                sync.finish(1);
            }
        }
    }

    /**
     * Window iterator wrapper which prevent returning more elements that existed in the underlying collection by the
     * time of iterator creation.
     */
    private class BoundedIterator implements Iterator<E> {
        /** Iterator. */
        private final GridStreamerWindowIterator<E> iter;

        /** How many elements to return left (at most). */
        private int left;

        /**
         * Constructor.
         *
         * @param iter Iterator.
         */
        private BoundedIterator(GridStreamerWindowIterator<E> iter) {
            assert iter != null;
            assert lock != null;

            this.iter = iter;

            left = size();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return left > 0 && iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public E next() {
            left--;

            if (left < 0)
                throw new NoSuchElementException();

            return iter.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (left < 0)
                throw new IllegalStateException();

            lock.readLock();

            try {
                E evt = iter.removex();

                if (evt != null) {
                    try {
                        updateIndexes(evt, true);
                    }
                    catch (GridException e) {
                        throw new GridRuntimeException("Faied to remove event: " + evt, e);
                     }
                }
            }
            finally {
                lock.readUnlock();
            }
        }
    }
}
