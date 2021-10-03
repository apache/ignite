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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;

/**
 * Base class for distributed cache query reducers.
 */
public abstract class DistributedCacheQueryReducer<T> extends CacheQueryReducer<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Page streams collection. */
    protected final Map<UUID, NodePageStream<T>> pageStreams;

    /** Timestamp of query timeout. */
    protected final long endTime;

    /** */
    protected DistributedCacheQueryReducer(final Map<UUID, NodePageStream<T>> pageStreams, long endTime) {
        this.pageStreams = pageStreams;
        this.endTime = endTime > 0 ? endTime : Long.MAX_VALUE;
    }

    /**
     * @return Page with query results data from specified stream. In case of error return empty page.
     */
    protected NodePage<T> page(UUID nodeId, CompletableFuture<?> pageFut) throws IgniteCheckedException {
        try {
            long timeout = endTime - System.currentTimeMillis();

            if (timeout < 0)
                return new NodePage<>(nodeId, Collections.emptyList());

            nodeId = (UUID) pageFut.get(timeout, TimeUnit.MILLISECONDS);

            return pageStreams.get(nodeId).nextPage();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Query was interrupted.", e);
        }
        catch (TimeoutException | ExecutionException e) {
            return new NodePage<>(nodeId, Collections.emptyList());
        }
    }
}
