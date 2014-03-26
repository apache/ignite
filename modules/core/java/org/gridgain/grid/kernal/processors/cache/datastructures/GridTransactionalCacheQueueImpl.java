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
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * {@link GridCacheQueue} implementation using transactional cache.
 */
public class GridTransactionalCacheQueueImpl<T> extends GridCacheQueueAdapter<T> {
    /**
     * @param queueName Queue name.
     * @param uuid Queue UUID.
     * @param cctx Cache context.
     */
    public GridTransactionalCacheQueueImpl(String queueName, GridUuid uuid, GridCacheContext<?, ?> cctx) {
        super(queueName, uuid, cctx);
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(int batchSize) throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item) throws GridRuntimeException {
        try {
            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Long idx = (Long)cache.transformCompute(queueKey, new AddClosure());

                if (idx == null)
                    return false;

                boolean putx = cache.putx(new ItemKey(uuid, idx), item, null);

                assert putx;

                tx.commit();

                return true;
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(T item, long timeout, TimeUnit unit) throws GridRuntimeException {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll() throws GridRuntimeException {
        try {
            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Long idx = (Long)cache.transformCompute(queueKey, new PollClosure());

                if (idx == null)
                    return null;

                T e = (T)cache.remove(new ItemKey(uuid, idx), null);

                assert e != null;

                tx.commit();

                return e;
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() throws GridRuntimeException {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(long timeout, TimeUnit unit) throws GridRuntimeException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void clear(int batchSize) throws GridRuntimeException {
    }

    /** {@inheritDoc} */
    @Override public int capacity() throws GridException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean bounded() throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
    }
}
