/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * {@link GridCacheQueue} implementation using transactional cache.
 */
public class GridTransactionalCacheQueueImpl<T> extends GridCacheQueueAdapter<T> {
    /**
     * @param queueName Queue name.
     * @param hdr Queue header.
     * @param cctx Cache context.
     */
    public GridTransactionalCacheQueueImpl(String queueName, GridCacheQueueHeader hdr, GridCacheContext<?, ?> cctx) {
        super(queueName, hdr, cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean offer(final T item) throws IgniteException {
        A.notNull(item, "item");

        try {
            boolean retVal;

            int cnt = 0;

            while (true) {
                try {
                    try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.transformAndCompute(queueKey, new AddClosure(id, 1));

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
                catch (ClusterGroupEmptyException e) {
                    throw e;
                }
                catch (ClusterTopologyException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return retVal;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws IgniteException {
        try {
            int cnt = 0;

            T retVal;

            while (true) {
                try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.transformAndCompute(queueKey, new PollClosure(id));

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
                catch (ClusterGroupEmptyException e) {
                    throw e;
                }
                catch(ClusterTopologyException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to poll, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return retVal;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean addAll(final Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            boolean retVal;

            int cnt = 0;

            while (true) {
                try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.transformAndCompute(queueKey, new AddClosure(id, items.size()));

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
                catch (ClusterGroupEmptyException e) {
                    throw e;
                }
                catch(ClusterTopologyException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to addAll, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return retVal;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(final long rmvIdx) throws IgniteCheckedException {
        try {
            int cnt = 0;

            while (true) {
                try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.transformAndCompute(queueKey, new RemoveClosure(id, rmvIdx));

                    if (idx != null) {
                        checkRemoved(idx);

                        boolean rmv = cache.removex(itemKey(idx));

                        assert rmv;
                    }

                    tx.commit();

                    break;
                }
                catch (ClusterGroupEmptyException e) {
                    throw e;
                }
                catch(ClusterTopologyException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to remove item, will retry [err=" + e + ", idx=" + rmvIdx + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
