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

import java.util.UUID;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Reducer for distributed cache query.
 */
public interface DistributedCacheQueryReducer<T> extends CacheQueryReducer<T> {
    /**
     * Loads full cache query result pages from remote nodes. It can be done for speedup operation if user invokes
     * get() on {@link GridCacheQueryFutureAdapter} instead of using it as iterator.
     *
     * @throws IgniteInterruptedCheckedException If thread is interrupted.
     */
    public void loadAll() throws IgniteInterruptedCheckedException;

    /**
     * Checks whether cache query runs on specified node.
     *
     * @param nodeId Node ID.
     * @return {@code true} if specified node runs this query.
     */
    public boolean queryNode(UUID nodeId);

    /** Blocks while reducer doesn't get first result item for this query. */
    public void awaitFirstItem() throws IgniteInterruptedCheckedException;

    /** Callback that invokes when this query is cancelled. */
    public void onCancel();

    /**
     * Callback that invokes after reducer get last query result page.
     * Also invokes for failed queries to let reducer know that there won't be new pages.
     */
    public void onLastPage();
}
