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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheLocalQueryFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Simple reducer for local queries. */
public class LocalCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Iterator provides access to result data. Local query returns all data in single page. */
    private final GridFutureAdapter<Iterator<R>> pageFut;

    /** */
    private Iterator<R> page;

    /** */
    private final GridCacheLocalQueryFuture<?, ?, R> fut;

    /** */
    public LocalCacheQueryReducer(GridCacheLocalQueryFuture<?, ?, R> fut, GridFutureAdapter<Iterator<R>> pageFut) {
        this.pageFut = pageFut;
        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        Iterator<R> p = page;

        if (p == null) {
            long waitTime = fut.query().query().timeout() == 0 ? Long.MAX_VALUE : fut.endTime() - U.currentTimeMillis();

            try {
                page = p = pageFut.get(waitTime, TimeUnit.MILLISECONDS);

            } catch (IgniteCheckedException e) {
                p = Collections.emptyIterator();
            }
        }

        return p != null && p.hasNext();
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
        return page.next();
    }
}
