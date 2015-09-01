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
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * {@link org.apache.ignite.IgniteQueue} implementation using atomic cache.
 */
public class GridAtomicCacheQueueImpl<T> extends GridCacheQueueAdapter<T> {
    /** */
    private static final long RETRY_TIMEOUT = 3000;

    /**
     * @param queueName Queue name.
     * @param hdr Queue header.
     * @param cctx Cache context.
     */
    public GridAtomicCacheQueueImpl(String queueName, GridCacheQueueHeader hdr, GridCacheContext<?, ?> cctx) {
        super(queueName, hdr, cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean offer(T item) throws IgniteException {
        try {
            Long idx = transformHeader(new AddProcessor(id, 1));

            if (idx == null)
                return false;

            checkRemoved(idx);

            int cnt = 0;

            GridCacheQueueItemKey key = itemKey(idx);

            while (true) {
                try {
                    cache.getAndPut(key, item);

                    break;
                }
                catch (CachePartialUpdateCheckedException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to put queue item, will retry [err=" + e + ", idx=" + idx + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return true;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws IgniteException {
        try {
            while (true) {
                Long idx = transformHeader(new PollProcessor(id));

                if (idx == null)
                    return null;

                checkRemoved(idx);

                GridCacheQueueItemKey key = itemKey(idx);

                int cnt = 0;

                long stop = 0;

                while (true) {
                    try {
                        T data = (T)cache.getAndRemove(key);

                        if (data != null)
                            return data;

                        if (stop == 0)
                            stop = U.currentTimeMillis() + RETRY_TIMEOUT;

                        while (U.currentTimeMillis() < stop ) {
                            data = (T)cache.getAndRemove(key);

                            if (data != null)
                                return data;
                        }

                        break;
                    }
                    catch (CachePartialUpdateCheckedException e) {
                        if (cnt++ == MAX_UPDATE_RETRIES)
                            throw e;
                        else {
                            U.warn(log, "Failed to remove queue item, will retry [err=" + e + ']');

                            U.sleep(RETRY_DELAY);
                        }
                    }
                }

                U.warn(log, "Failed to get item, will retry poll [queue=" + queueName + ", idx=" + idx + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean addAll(Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            Long idx = transformHeader(new AddProcessor(id, items.size()));

            if (idx == null)
                return false;

            checkRemoved(idx);

            Map<GridCacheQueueItemKey, T> putMap = new HashMap<>();

            for (T item : items) {
                putMap.put(itemKey(idx), item);

                idx++;
            }

            int cnt = 0;

            while (true) {
                try {
                    cache.putAll(putMap);

                    break;
                }
                catch (CachePartialUpdateCheckedException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to add items, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            return true;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(long rmvIdx) throws IgniteCheckedException {
        Long idx = (Long)cache.invoke(queueKey, new RemoveProcessor(id, rmvIdx)).get();

        if (idx != null) {
            checkRemoved(idx);

            GridCacheQueueItemKey key = itemKey(idx);

            int cnt = 0;

            long stop = 0;

            while (true) {
                try {
                    if (cache.remove(key))
                        return;

                    if (stop == 0)
                        stop = U.currentTimeMillis() + RETRY_TIMEOUT;

                    while (U.currentTimeMillis() < stop ) {
                        if (cache.remove(key))
                            return;
                    }

                    break;
                }
                catch (CachePartialUpdateCheckedException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to add items, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }

            U.warn(log, "Failed to remove item, [queue=" + queueName + ", idx=" + idx + ']');
        }
    }

    /**
     * @param c EntryProcessor to be applied for queue header.
     * @return Value computed by the entry processor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable private Long transformHeader(EntryProcessor<GridCacheQueueHeaderKey, GridCacheQueueHeader, Long> c)
        throws IgniteCheckedException {
        int cnt = 0;

        while (true) {
            try {
                return (Long)cache.invoke(queueKey, c).get();
            }
            catch (CachePartialUpdateCheckedException e) {
                if (cnt++ == MAX_UPDATE_RETRIES)
                    throw e;
                else {
                    U.warn(log, "Failed to update queue header, will retry [err=" + e + ']');

                    U.sleep(RETRY_DELAY);
                }
            }
        }
    }
}