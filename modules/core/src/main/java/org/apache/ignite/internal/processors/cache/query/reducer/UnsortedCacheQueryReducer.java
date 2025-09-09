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
 * Reducer of cache query results, no ordering of results is provided.
 */
public class UnsortedCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Current page to return data to user. */
    private NodePage<R> page;

    /** Pending futures for requeseted pages. */
    private final CompletableFuture<NodePage<R>>[] futs;

    /** */
    public UnsortedCacheQueryReducer(Map<UUID, NodePageStream<R>> pageStreams) {
        super(pageStreams);

        futs = new CompletableFuture[pageStreams.size()];
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        while (page == null || !page.hasNext()) {
            int pendingNodesCnt = 0;

            for (NodePageStream<R> s: pageStreams.values()) {
                if (s.closed())
                    continue;

                CompletableFuture<NodePage<R>> f = s.headPage();

                if (f.isDone()) {
                    page = get(f);

                    if (page.hasNext())
                        return true;
                }
                else
                    futs[pendingNodesCnt++] = f;
            }

            if (pendingNodesCnt == 0)
                return false;

            CompletableFuture[] pendingFuts = Arrays.copyOf(futs, pendingNodesCnt);

            Arrays.fill(futs, 0, pendingNodesCnt, null);

            page = get(CompletableFuture.anyOf(pendingFuts));
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }
}
