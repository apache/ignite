/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Cache query future returned by query execution.
 * Refer to {@link CacheQuery} documentation for more information.
 */
public interface CacheQueryFuture<T> extends IgniteInternalFuture<Collection<T>>, AutoCloseable {
    /**
     * Returns next element from result set.
     * <p>
     * This is a blocking call which will wait if there are no
     * elements available immediately.
     *
     * @return Next fetched element or {@code null} if all the elements have been fetched.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public T next() throws IgniteCheckedException;

    /**
     * Checks if all data is fetched by the query.
     *
     * @return {@code True} if all data is fetched, {@code false} otherwise.
     */
    @Override public boolean isDone();

    /**
     * Cancels this query future and stop receiving any further results for the query
     * associated with this future.
     *
     * @return {@inheritDoc}
     * @throws IgniteCheckedException {@inheritDoc}
     */
    @Override public boolean cancel() throws IgniteCheckedException;
}
