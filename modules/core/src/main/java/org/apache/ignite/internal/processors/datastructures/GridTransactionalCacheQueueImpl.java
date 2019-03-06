/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.datastructures;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.retryTopologySafe;
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
            return retryTopologySafe(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    boolean retVal;

                    try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, 1)).get();

                        if (idx != null) {
                            checkRemoved(idx);

                            cache.getAndPut(itemKey(idx), item);

                            retVal = true;
                        }
                        else
                            retVal = false;

                        tx.commit();

                        return retVal;
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws IgniteException {
        try {
            return retryTopologySafe(new Callable<T>() {
                @Override public T call() throws Exception {
                    T retVal;

                    while (true) {
                        try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                            Long idx = (Long)cache.invoke(queueKey, new PollProcessor(id)).get();

                            if (idx != null) {
                                checkRemoved(idx);

                                retVal = (T)cache.getAndRemove(itemKey(idx));

                                if (retVal == null) { // Possible if data was lost.
                                    tx.commit();

                                    continue;
                                }
                            }
                            else
                                retVal = null;

                            tx.commit();

                            return retVal;
                        }
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean addAll(final Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            return retryTopologySafe(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    boolean retVal;

                    try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, items.size())).get();

                        if (idx != null) {
                            checkRemoved(idx);

                            Map<QueueItemKey, T> putMap = new HashMap<>();

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

                        return retVal;
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(final long rmvIdx) throws IgniteCheckedException {
        try {
            retryTopologySafe(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new RemoveProcessor(id, rmvIdx)).get();

                        if (idx != null) {
                            checkRemoved(idx);

                            cache.remove(itemKey(idx));
                        }

                        tx.commit();
                    }

                    return null;
                }
            });
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }
}
