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
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.CacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;

/** Simple reducer for local queries. */
public class LocalCacheQueryReducer<R> implements CacheQueryReducer<R> {
    /** Stream of local pages. */
    private final PageStream<R> pageStream;

    /** */
    public LocalCacheQueryReducer(GridCacheQueryAdapter qry, Object queueLock, long timeoutTime) {
        pageStream = new PageStream<>(qry, queueLock, timeoutTime, Collections.emptyList(), (ns, all) -> {});
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
    @Override public boolean onPage(UUID nodeId, Collection<R> data, boolean last) {
        return pageStream.addPage(nodeId, data, last);
    }

    /** {@inheritDoc} */
    @Override public void onError() {
        pageStream.onError();
    }
}
