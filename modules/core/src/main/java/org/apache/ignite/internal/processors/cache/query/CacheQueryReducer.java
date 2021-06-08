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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;

/**
 * This class is responsible for reducing results of cache query. Query results are delivered with function
 * {@link #onPage(UUID, Collection, boolean)}. Note that this reducer deeply interacts with corresponding query future
 * {@link GridCacheQueryFutureAdapter}, so they used the same lock object. It guards reducer pages operations
 * and the future status. Custom reduce logic is applied within {@link #next()} and {@link #hasNext()}.
 *
 * <T> is a type of cache query result item.
 */
public interface CacheQueryReducer<T> {
    /**
     * @return Next item.
     */
    public T next() throws IgniteCheckedException;

    /**
     * @return {@code true} if there is a next item, otherwise {@code false}.
     */
    public boolean hasNext() throws IgniteCheckedException;

    /**
     * Offer query result page for reduce. Note that the data collection may contain extension of type T.
     * In such cases data item contains additional payload for custom reducer logic ({@see CacheEntryWithPayload}).
     *
     * @param nodeId Node ID that sent this page.
     * @param data Page data rows.
     * @param last Whether this page is last for specified {@code nodeId}.
     * @return {@code true} if this page is final page for query and no more pages are waited, otherwise {@code false}.
     */
    public boolean onPage(UUID nodeId, Collection<T> data, boolean last);

    /**
     * Callback in case of page with error.
     */
    public void onError();
}
