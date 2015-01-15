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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@link GridCacheQueue} implementation using atomic cache.
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
            Long idx = transformHeader(new AddClosure(id, 1));

            if (idx == null)
                return false;

            checkRemoved(idx);

            int cnt = 0;

            GridCacheQueueItemKey key = itemKey(idx);

            while (true) {
                try {
                    boolean putx = cache.putx(key, item, null);

                    assert putx;

                    break;
                }
                catch (GridCachePartialUpdateException e) {
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
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public T poll() throws IgniteException {
        try {
            while (true) {
                Long idx = transformHeader(new PollClosure(id));

                if (idx == null)
                    return null;

                checkRemoved(idx);

                GridCacheQueueItemKey key = itemKey(idx);

                int cnt = 0;

                long stop = 0;

                while (true) {
                    try {
                        T data = (T)cache.remove(key, null);

                        if (data != null)
                            return data;

                        if (stop == 0)
                            stop = U.currentTimeMillis() + RETRY_TIMEOUT;

                        while (U.currentTimeMillis() < stop ) {
                            data = (T)cache.remove(key, null);

                            if (data != null)
                                return data;
                        }

                        break;
                    }
                    catch (GridCachePartialUpdateException e) {
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
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean addAll(Collection<? extends T> items) {
        A.notNull(items, "items");

        try {
            Long idx = transformHeader(new AddClosure(id, items.size()));

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
                    cache.putAll(putMap, null);

                    break;
                }
                catch (GridCachePartialUpdateException e) {
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
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void removeItem(long rmvIdx) throws IgniteCheckedException {
        Long idx = (Long)cache.transformAndCompute(queueKey, new RemoveClosure(id, rmvIdx));

        if (idx != null) {
            checkRemoved(idx);

            GridCacheQueueItemKey key = itemKey(idx);

            int cnt = 0;

            long stop = 0;

            while (true) {
                try {
                    if (cache.removex(key))
                        return;

                    if (stop == 0)
                        stop = U.currentTimeMillis() + RETRY_TIMEOUT;

                    while (U.currentTimeMillis() < stop ) {
                        if (cache.removex(key))
                            return;
                    }

                    break;
                }
                catch (GridCachePartialUpdateException e) {
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
     * @param c Transform closure to be applied for queue header.
     * @return Value computed by the transform closure.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable private Long transformHeader(IgniteClosure<GridCacheQueueHeader, IgniteBiTuple<GridCacheQueueHeader, Long>> c)
        throws IgniteCheckedException {
        int cnt = 0;

        while (true) {
            try {
                return (Long)cache.transformAndCompute(queueKey, c);
            }
            catch (GridCachePartialUpdateException e) {
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
