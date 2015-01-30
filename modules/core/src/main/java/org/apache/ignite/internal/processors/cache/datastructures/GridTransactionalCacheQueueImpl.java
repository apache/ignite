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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * {@link org.apache.ignite.cache.datastructures.CacheQueue} implementation using transactional cache.
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
                    try (IgniteTxEx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, 1)).get();

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
                catch (ClusterTopologyCheckedException e) {
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
                try (IgniteTxEx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new PollProcessor(id)).get();

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
                catch (ClusterTopologyCheckedException e) {
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
    @Override public boolean addAll(final Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            boolean retVal;

            int cnt = 0;

            while (true) {
                try (IgniteTxEx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new AddProcessor(id, items.size())).get();

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
                catch (ClusterTopologyCheckedException e) {
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
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(final long rmvIdx) throws IgniteCheckedException {
        try {
            int cnt = 0;

            while (true) {
                try (IgniteTxEx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Long idx = (Long)cache.invoke(queueKey, new RemoveProcessor(id, rmvIdx)).get();

                    if (idx != null) {
                        checkRemoved(idx);

                        boolean rmv = cache.removex(itemKey(idx));

                        assert rmv;
                    }

                    tx.commit();

                    break;
                }
                catch (ClusterTopologyCheckedException e) {
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
