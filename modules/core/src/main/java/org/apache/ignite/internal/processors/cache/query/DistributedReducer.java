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
 * Reduce cache query results from remote nodes that executes distributed query.
 */
public interface DistributedReducer<T> extends Reducer<T> {
    /**
     * Callback that invoked after handling a page from remote node.
     *
     * @param nodeId Node ID of remote page.
     * @param last Whether page is last for specified node.
     * @return Whether page is last for a query.
     */
    public boolean onPage(UUID nodeId, boolean last);

    /**
     * Loads full cache query result pages from remote nodes. It can be done for speedup operation if user invokes
     * get() on {@link GridCacheQueryFutureAdapter} instead of using it as iterator.
     *
     * @throws IgniteInterruptedCheckedException If thread is interrupted.
     */
    public void loadAll() throws IgniteInterruptedCheckedException;

    /**
     * Callback to handle node leaving.
     *
     * @param nodeId Node ID that left a cluster.
     * @return {@code true} if specified node runs this query.
     */
    public boolean onNodeLeft(UUID nodeId);

    /** Blocks while reducer doesn't get first result item for this query. */
    public void awaitFirstItem() throws InterruptedException;

    /** Callback that invokes when this query is cancelled. */
    public void cancel();
}
