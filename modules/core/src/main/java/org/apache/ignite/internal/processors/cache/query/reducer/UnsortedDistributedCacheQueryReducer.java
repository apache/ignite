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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteCheckedException;

/**
 * Reducer of distributed query, fetch pages from remote nodes. All pages go in single page stream so no ordering is provided.
 */
public class UnsortedDistributedCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Current page to return data to user. */
    private NodePage<R> page;

    /** */
    public UnsortedDistributedCacheQueryReducer(Map<UUID, NodePageStream<R>> pageStreams) {
        super(pageStreams);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        while (page == null || !page.hasNext()) {
            CompletableFuture<NodePage<R>>[] futs = new CompletableFuture[pageStreams.size()];

            int pendingNodesCnt = 0;

            for (NodePageStream<R> s: pageStreams.values()) {
                if (s.closed())
                    continue;

                CompletableFuture<NodePage<R>> f = s.headPage();

                if (f.isDone()) {
                    page = page(f);

                    if (page.hasNext())
                        return true;
                } else
                    futs[pendingNodesCnt++] = f;
            }

            if (pendingNodesCnt == 0)
                return false;

            page = page(CompletableFuture.anyOf(Arrays.copyOf(futs, pendingNodesCnt)));
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }
}
