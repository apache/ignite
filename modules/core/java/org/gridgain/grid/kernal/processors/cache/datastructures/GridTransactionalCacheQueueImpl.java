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
import org.gridgain.grid.util.typedef.internal.*;
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
     * @param header Queue header.
     * @param cctx Cache context.
     * @throws GridException If failed.
     */
    public GridTransactionalCacheQueueImpl(String queueName, GridCacheQueueHeader header, GridCacheContext<?, ?> cctx)
        throws GridException {
        super(queueName, header, cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean offer(final T item) throws GridRuntimeException {
        A.notNull(item, "item");

        try {
            return CU.outTx(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    boolean retVal;

                    int cnt = 0;

                    while (true) {
                        try {
                            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                Long idx = (Long)cache.transformCompute(queueKey, new AddClosure(uuid, 1));

                                if (idx != null) {
                                    checkRemoved(idx);

                                    boolean putx = cache.putx(itemKey(idx), item, null);

                                    assert putx;

                                    retVal = true;
                                }
                                else
                                    retVal = false;

                                tx.commit();

                                break;
                            }
                        }
                        catch (GridEmptyProjectionException e) {
                            throw e;
                        }
                        catch (GridTopologyException e) {
                            if (cnt++ == MAX_UPDATE_RETRIES)
                                throw e;
                            else
                                U.warn(log, "Failed to add item, will retry [err=" + e + ']');
                        }
                    }

                    return retVal;
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws GridRuntimeException {
        try {
            return CU.outTx(new Callable<T>() {
                @Override public T call() throws Exception {
                    int cnt = 0;

                    T retVal;

                    while (true) {
                        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Long idx = (Long)cache.transformCompute(queueKey, new PollClosure(uuid));

                            if (idx != null) {
                                checkRemoved(idx);

                                retVal = (T)cache.remove(itemKey(idx), null);

                                assert retVal != null;
                            }
                            else
                                retVal = null;

                            tx.commit();

                            break;
                        }
                        catch (GridEmptyProjectionException e) {
                            throw e;
                        }
                        catch(GridTopologyException e) {
                            if (cnt++ == MAX_UPDATE_RETRIES)
                                throw e;
                            else
                                U.warn(log, "Failed to poll, will retry [err=" + e + ']');
                        }
                    }

                    return retVal;
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean addAll(final Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            return CU.outTx(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    boolean retVal;

                    int cnt = 0;

                    while (true) {
                        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Long idx = (Long)cache.transformCompute(queueKey, new AddClosure(uuid, items.size()));

                            if (idx != null) {
                                checkRemoved(idx);

                                Map<GridCacheQueueItemKey, T> putMap = new HashMap<>();

                                for (T item : items) {
                                    putMap.put(itemKey(idx), item);

                                    idx++;
                                }

                                cache.putAll(putMap, null);

                                retVal = true;
                            }
                            else
                                retVal = false;

                            tx.commit();

                            break;
                        }
                        catch (GridEmptyProjectionException e) {
                            throw e;
                        }
                        catch(GridTopologyException e) {
                            if (cnt++ == MAX_UPDATE_RETRIES)
                                throw e;
                            else
                                U.warn(log, "Failed to addAll, will retry [err=" + e + ']');
                        }
                    }

                    return retVal;
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(final long rmvIdx) throws GridException {
        try {
            CU.outTx(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int cnt = 0;

                    while (true) {
                        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Long idx = (Long)cache.transformCompute(queueKey, new RemoveClosure(uuid, rmvIdx));

                            if (idx != null) {
                                checkRemoved(idx);

                                boolean rmv = cache.removex(itemKey(idx));

                                assert rmv;
                            }

                            tx.commit();

                            break;
                        }
                        catch (GridEmptyProjectionException e) {
                            throw e;
                        }
                        catch(GridTopologyException e) {
                            if (cnt++ == MAX_UPDATE_RETRIES)
                                throw e;
                            else
                                U.warn(log, "Failed to remove item, will retry [err=" + e + ", idx=" + rmvIdx + ']');
                        }
                    }

                    return null;
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(int batchSize) throws GridRuntimeException {
        try {
            CU.outTx(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    GridTransactionalCacheQueueImpl.super.clear();

                    return null;
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T peek() throws GridRuntimeException {
        try {
            return CU.outTx(new Callable<T>() {
                @Override public T call() throws Exception {
                    return GridTransactionalCacheQueueImpl.super.peek();
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        try {
            return CU.outTx(new Callable<Integer>() {
                @Override public Integer call() throws Exception {
                    return GridTransactionalCacheQueueImpl.super.size();
                }
            }, cctx);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }
}
