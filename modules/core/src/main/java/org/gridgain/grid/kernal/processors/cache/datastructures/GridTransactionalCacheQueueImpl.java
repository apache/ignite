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
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * {@link GridCacheQueue} implementation using transactional cache.
 */
public class GridTransactionalCacheQueueImpl<T> extends GridCacheQueueAdapter<T> {
    /** */
    private final IgniteTransactions txs;

    /**
     * @param queueName Queue name.
     * @param hdr Queue header.
     * @param cctx Cache context.
     */
    public GridTransactionalCacheQueueImpl(String queueName, GridCacheQueueHeader hdr, GridCacheContext<?, ?> cctx) {
        super(queueName, hdr, cctx);

        txs = cctx.kernalContext().grid().transactions();
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
                    try (IgniteTx tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, 1));

                        if (idx != null) {
                            checkRemoved(idx);

                            cache.put(itemKey(idx), item);

                            retVal = true;
                        }
                        else
                            retVal = false;

                        tx.commit();

                        break;
                    }
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof ClusterGroupEmptyException)
                        throw e;

                    if (e.getCause() instanceof ClusterTopologyException) {
                        if (cnt++ == MAX_UPDATE_RETRIES)
                            throw e;
                        else {
                            U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                            U.sleep(RETRY_DELAY);
                        }
                    }
                    else
                        throw e;
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
                try (IgniteTx tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new PollProcessor(id));

                    if (idx != null) {
                        checkRemoved(idx);

                        retVal = (T)cache.getAndRemove(itemKey(idx));

                        assert retVal != null;
                    }
                    else
                        retVal = null;

                    tx.commit();

                    break;
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof ClusterGroupEmptyException)
                        throw e;

                    if (e.getCause() instanceof ClusterTopologyException) {
                        if (cnt++ == MAX_UPDATE_RETRIES)
                            throw e;
                        else {
                            U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                            U.sleep(RETRY_DELAY);
                        }
                    }
                    else
                        throw e;
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
                try (IgniteTx tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, items.size()));

                    if (idx != null) {
                        checkRemoved(idx);

                        Map<GridCacheQueueItemKey, T> putMap = new HashMap<>();

                        for (T item : items) {
                            putMap.put(itemKey(idx), item);

                            idx++;
                        }

                        cache.putAll(putMap);

                        retVal = true;
                    }
                    else
                        retVal = false;

                    tx.commit();

                    break;
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof ClusterGroupEmptyException)
                        throw e;

                    if (e.getCause() instanceof ClusterTopologyException) {
                        if (cnt++ == MAX_UPDATE_RETRIES)
                            throw e;
                        else {
                            U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                            U.sleep(RETRY_DELAY);
                        }
                    }
                    else
                        throw e;
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
                try (IgniteTx tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new RemoveProcessor(id, rmvIdx));

                    if (idx != null) {
                        checkRemoved(idx);

                        boolean rmv = cache.remove(itemKey(idx));

                        assert rmv;
                    }

                    tx.commit();

                    break;
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof ClusterGroupEmptyException)
                        throw e;

                    if (e.getCause() instanceof ClusterTopologyException) {
                        if (cnt++ == MAX_UPDATE_RETRIES)
                            throw e;
                        else {
                            U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                            U.sleep(RETRY_DELAY);
                        }
                    }
                    else
                        throw e;
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
