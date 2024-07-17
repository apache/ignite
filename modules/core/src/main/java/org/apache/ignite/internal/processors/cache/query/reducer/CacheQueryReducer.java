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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;

/**
 * This abstract class is base class for cache query reducers. They are responsible for reducing results of cache query.
 *
 * <T> is a type of cache query result item.
 */
public abstract class CacheQueryReducer<T> extends GridIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Page streams collection. */
    protected final Map<UUID, NodePageStream<T>> pageStreams;

    /** Maximum number of entries to deliver, {@code 0} if query result isn't limited. */
    protected final int limit;

    /** Counter of delivered items. */
    private final AtomicInteger cnt;

    /** */
    // TODO: test limit 0, -10
    protected CacheQueryReducer(final Map<UUID, NodePageStream<T>> pageStreams, int limit) {
        this.pageStreams = pageStreams;
        this.limit = limit;

        cnt = limitEnabled() ? new AtomicInteger() : null;
    }

    /** {@inheritDoc} */
    @Override public final boolean hasNextX() throws IgniteCheckedException {
        if (limitEnabled() && cnt.get() == limit)
            return false;

        return hasNext0();
    }

    /** {@inheritDoc} */
    @Override public final T nextX() {
        if (limitEnabled())
            cnt.incrementAndGet();

        return next0();
    }

    /**
     * Handles a new data page from specified node.
     *
     * @param nodeId Node ID of new data page.
     * @param data Data page.
     * @param last Whether it's a last page from node {@code nodeId}.
     * @return {@code true} if query limit is specified and reducer has enough data to satisfy it, otherwise {@code false}.
     */
    public final boolean addPage(UUID nodeId, Collection<T> data, boolean last) {
        NodePageStream<T> stream = pageStreams.get(nodeId);

        if (stream == null)
            return false;

        stream.addPage(data, last);

        if (limitEnabled()) {
            boolean cancelRemote = checkLimit(cnt.get());

            if (cancelRemote) {
                for (NodePageStream<T> s: pageStreams.values()) {
                    if (s.closed())
                        continue;

                    s.cancel(null);
                }
            }

            return cancelRemote;
        }

        return false;
    }

    /**
     * Checks underlying page streams for next element.
     *
     * @return {@code true} if reducer can deliver next element, otherwise {@code false}.
     */
    protected abstract boolean hasNext0() throws IgniteCheckedException;

    /**
     * Return next element from underlying page streams.
     *
     * @return Next element.
     */
    protected abstract T next0();

    /** {@inheritDoc} */
    @Override public final void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("CacheQueryReducer doesn't support removing items.");
    }

    /**
     * Checks whether the underlying streams locally contain enough data for satisfying {@code limit}.
     *
     * @return {@code true} if streams already have enough data for satisfying limit, otherwise {@code false}.
     */
    protected abstract boolean checkLimit(int cnt);

    /**
     * @return Object that completed the specified future.
     * @throws IgniteCheckedException for all failures.
     */
    public static <T> T get(CompletableFuture<?> fut) throws IgniteCheckedException {
        try {
            return (T)fut.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Query was interrupted.", e);
        }
        catch (ExecutionException e) {
            Throwable t = e.getCause();

            if (t instanceof IgniteCheckedException)
                throw (IgniteCheckedException)t;

            throw new IgniteCheckedException("Page future was completed with unexpected error.", e);
        }
    }

    /** */
    protected final boolean limitEnabled() {
        return limit > 0;
    }
}
