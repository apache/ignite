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
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheLocalQueryFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Simple reducer for local queries. */
public class LocalCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Iterator provides access to result data. Local query returns all data in single page. */
    private volatile Iterator<R> page;

    /** */
    private final GridCacheLocalQueryFuture<?, ?, R> fut;

    /** Guards page. */
    private final Object pagesLock = new Object();

    /** */
    public LocalCacheQueryReducer(GridCacheLocalQueryFuture<?, ?, R> fut) {
        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        Iterator<R> p = page;

        if (p != null)
            return p.hasNext();

        while (page == null) {
            long timeout = fut.query().query().timeout();

            long waitTime = timeout == 0 ? Long.MAX_VALUE : fut.endTime() - U.currentTimeMillis();

            if (waitTime <= 0)
                return false;

            synchronized (pagesLock) {
                try {
                    if (page == null)
                        pagesLock.wait(waitTime);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteCheckedException("Query was interrupted: " + fut.query().query(), e);
                }
            }
        }

        return page.hasNext();
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }

    /** Receive the only page with data for local reducer. */
    public void onPage(UUID nodeId, Collection<R> data, boolean last) {
        synchronized (pagesLock) {
            page = data.iterator();

            pagesLock.notifyAll();
        }
    }

    /** */
    public void onError() {
        onPage(null, Collections.emptyList(), true);
    }
}
