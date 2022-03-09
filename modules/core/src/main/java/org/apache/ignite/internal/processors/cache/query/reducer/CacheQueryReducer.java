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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

    /** */
    protected CacheQueryReducer(final Map<UUID, NodePageStream<T>> pageStreams) {
        this.pageStreams = pageStreams;
    }

    /** {@inheritDoc} */
    @Override public void removeX() throws IgniteCheckedException {
        throw new UnsupportedOperationException("CacheQueryReducer doesn't support removing items.");
    }

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
}
