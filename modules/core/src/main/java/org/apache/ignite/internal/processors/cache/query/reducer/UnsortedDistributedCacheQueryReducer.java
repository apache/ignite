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
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Reducer of distributed query, fetch pages from remote nodes. All pages go in single page stream so no ordering is provided.
 */
public class UnsortedDistributedCacheQueryReducer<R> extends DistributedCacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Current page to return data to user. */
    private NodePage<R> page;

    /**
     * @param nodes Collection of nodes this query applies to.
     */
    public UnsortedDistributedCacheQueryReducer(Function<UUID, NodePage<R>> pagesProvider, Collection<ClusterNode> nodes) {
        super(pagesProvider, nodes);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        if (page != null && page.hasNext())
            return true;

        Iterator<UUID> it = nodes.iterator();

        while (it.hasNext()) {
            NodePage<R> p = pagesProvider.apply(it.next());

            if (p == null || !p.hasNext())
                it.remove();
            else {
                page = p;

                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }
}
