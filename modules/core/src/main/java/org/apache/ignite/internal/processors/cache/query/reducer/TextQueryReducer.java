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

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;

/**
 * Reducer for {@code TextQuery} results.
 */
public class TextQueryReducer<R> extends MergeSortCacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public TextQueryReducer(final Map<UUID, NodePageStream<R>> pageStreams) {
        super(pageStreams);
    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Comparator<NodePage<R>>> pageComparator() {
        CompletableFuture<Comparator<NodePage<R>>> f = new CompletableFuture<>();

        f.complete((o1, o2) -> -Float.compare(
            ((ScoredCacheEntry<?, ?>)o1.head()).score(), ((ScoredCacheEntry<?, ?>)o2.head()).score()));

        return f;
    }
}
