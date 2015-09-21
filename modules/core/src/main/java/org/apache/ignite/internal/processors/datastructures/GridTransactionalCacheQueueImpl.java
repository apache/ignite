/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastructures;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * {@link IgniteQueue} implementation using transactional cache.
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
                    try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, 1)).get();

                        if (idx != null) {
                            checkRemoved(idx);

                            cache.getAndPut(itemKey(idx), item);

                            retVal = true;
                        }
                        else
                            retVal = false;

                        tx.commit();

                        break;
                    }
                }
                catch (ClusterTopologyCheckedException | TransactionRollbackException | IgniteTxRollbackCheckedException e) {
                    if (e instanceof ClusterGroupEmptyCheckedException)
                        throw e;

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
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws IgniteException {
        try {
            int cnt = 0;

            T retVal;

            while (true) {
                try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new PollProcessor(id)).get();

                    if (idx != null) {
                        checkRemoved(idx);

                        retVal = (T)cache.getAndRemove(itemKey(idx));

                        assert retVal != null : idx;
                    }
                    else
                        retVal = null;

                    tx.commit();

                    break;
                }
                catch (ClusterTopologyCheckedException | TransactionRollbackException | IgniteTxRollbackCheckedException e) {
                    if (e instanceof ClusterGroupEmptyCheckedException)
                        throw e;

                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to poll item, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return retVal;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
                try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, items.size())).get();

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
                catch (ClusterTopologyCheckedException | TransactionRollbackException | IgniteTxRollbackCheckedException e) {
                    if (e instanceof ClusterGroupEmptyCheckedException)
                        throw e;

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
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(final long rmvIdx) throws IgniteCheckedException {
        try {
            int cnt = 0;

            while (true) {
                try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new RemoveProcessor(id, rmvIdx)).get();

                    if (idx != null) {
                        checkRemoved(idx);

                        boolean rmv = cache.remove(itemKey(idx));

                        assert rmv : idx;
                    }

                    tx.commit();

                    break;
                }
                catch (ClusterTopologyCheckedException | TransactionRollbackException | IgniteTxRollbackCheckedException e) {
                    if (e instanceof ClusterGroupEmptyCheckedException)
                        throw e;

                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to add item, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}