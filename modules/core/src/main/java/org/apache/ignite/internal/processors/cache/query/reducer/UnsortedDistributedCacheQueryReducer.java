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
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
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
     * @param qryMgr Provides a functionality to request pages from remote nodes.
     * @param nodes Collection of nodes this query applies to.
     */
    public UnsortedDistributedCacheQueryReducer(
        GridCacheQueryFutureAdapter<?, ?, ?> fut, long reqId, GridCacheDistributedQueryManager<?, ?> qryMgr,
        Collection<ClusterNode> nodes) {
        super(fut, reqId, qryMgr, nodes);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        if (page != null && page.hasNext())
            return true;

        Iterator<NodePageStream> it = streams.values().iterator();

        while (it.hasNext()) {
            page = it.next().nextPage();

            if (page.hasNext())
                return true;

            it.remove();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        return page.next();
    }
}
