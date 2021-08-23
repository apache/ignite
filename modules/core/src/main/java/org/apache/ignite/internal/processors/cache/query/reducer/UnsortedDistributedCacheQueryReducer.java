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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;

/**
 * Reducer of distributed query, fetch pages from remote nodes. All pages go in single page stream so no ordering is provided.
 */
public class UnsortedDistributedCacheQueryReducer<R> extends AbstractDistributedCacheQueryReducer<R> {
    /** */
    private NodePage<R> page;

    /**
     * @param fut Cache query future.
     * @param reqId Cache query request ID.
     * @param pageRequester Provides a functionality to request pages from remote nodes.
     * @param queueLock Lock object that is shared between GridCacheQueryFuture and reducer.
     * @param nodes Collection of nodes this query applies to.
     */
    public UnsortedDistributedCacheQueryReducer(
        GridCacheQueryFutureAdapter fut, long reqId, CacheQueryPageRequester pageRequester,
        Object queueLock, Collection<ClusterNode> nodes) {
        super(fut, reqId, pageRequester, queueLock, nodes);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        if (page != null && page.hasNext())
            return true;

        Collection<NodePageStream<R>> streams = new ArrayList<>(this.streams.values());

        for (NodePageStream<R> s: streams) {
            page = s.nextPage();

            if (page != null && page.hasNext())
                return true;

            this.streams.remove(s.nodeId());
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        return page.next();
    }
}
