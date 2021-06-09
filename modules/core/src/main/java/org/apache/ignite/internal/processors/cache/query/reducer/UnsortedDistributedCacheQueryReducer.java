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
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Reducer of distributed query, fetch pages from remote nodes. All pages go in single page stream so no ordering is provided.
 */
public class UnsortedDistributedCacheQueryReducer<R> extends AbstractDistributedCacheQueryReducer<R> {
    /** Single page stream. */
    private final PageStream<R> pageStream;

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

        super(fut, reqId, pageRequester);

        Collection<UUID> subgrid = new HashSet<>();

        for (ClusterNode node : nodes)
            subgrid.add(node.id());

        pageStream = new PageStream<>(fut.query().query(), queueLock, fut.endTime(), subgrid, (ns, all) ->
            pageRequester.requestPages(reqId, fut, ns, all)
        );
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        return pageStream.hasNext();
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        return pageStream.next();
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(@Nullable UUID nodeId, Collection<R> data, boolean last) {
        boolean lastPageRcvd = pageStream.addPage(nodeId, data, last);

        // Receive by first page from every node.
        if (!loadAllowed() && pageStream.allPagesReady())
            onFirstItemReady();

        return lastPageRcvd;
    }

    /** {@inheritDoc} */
    @Override public void onError() {
        pageStream.onError();
    }

    /** {@inheritDoc} */
    @Override public void onCancel() {
        pageStream.cancel((ns) -> pageRequester.cancelQueryRequest(reqId, ns, fut.fields()));
    }

    /** {@inheritDoc} */
    @Override public void loadAll() throws IgniteInterruptedCheckedException {
        awaitFirstItem();

        if (loadAllowed())
            pageStream.loadAll();
    }

    /** {@inheritDoc} */
    @Override public boolean queryNode(UUID nodeId) {
        return pageStream.queryNode(nodeId);
    }
}
