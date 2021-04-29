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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This reducer uses single unordered queue for all nodes.
 * Reducer sends page requests for all nodes, handle all responses, then request all nodes again.
 */
public abstract class UnorderedReducer<R> implements Reducer<R> {
    /** Queue of data of results pages. */
    private final Queue<Collection<R>> queue = new LinkedList<>();

    /** Iterator over current page. */
    private Iterator<R> iter;

    /** Query */
    private final GridCacheQueryAdapter qry;

    /** Flag shows whether all pages are ready. */
    private volatile boolean allPagesReady;

    /** End time of query by timeout. */
    private final long timeoutTime;

    /** Lock shared between this reducer and future. */
    private final Object sharedLock;

    /** */
    UnorderedReducer(GridCacheQueryFutureAdapter fut) {
        qry = fut.qry.query();
        timeoutTime = fut.endTime();
        sharedLock = fut.lock;
    }

    /** {@inheritDoc} */
    @Override public Object sharedLock() {
        return sharedLock;
    }

    /** {@inheritDoc} */
    @Override public void addPage(CacheQueryResultPage<R> page) {
        assert Thread.holdsLock(sharedLock);

        queue.add(page.rows());
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        allPagesReady = true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        return internalIterator().hasNext();
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        return internalIterator().next();
    }

    /**
     * Returns iterator over one of result pages. If currently there is no available pages, then loads new and wait for it.
     *
     * @return Iterator over one of result pages.
     * @throws IgniteCheckedException In case of error.
     */
    private Iterator<R> internalIterator() throws IgniteCheckedException {
        Iterator<R> it = null;

        while (it == null || !it.hasNext()) {
            Collection<R> c;

            // Check current page iterator.
            synchronized (sharedLock) {
                it = iter;

                if (it != null && it.hasNext())
                    break;

                // Prev iterator is done. Try get new iterator over next page.
                c = queue.poll();

                if (c != null)
                    it = iter = c.iterator();

                // If all pages are ready, then skip wait for new pages.
                if (allPagesReady && queue.peek() == null)
                    break;
            }

            // Try load new page, and wait for that.
            if (c == null && !allPagesReady) {
                loadPage();

                long timeout = qry.timeout();

                long waitTime = timeout == 0 ? Long.MAX_VALUE : timeoutTime - U.currentTimeMillis();

                if (waitTime <= 0) {
                    it = Collections.<R>emptyList().iterator();

                    break;
                }

                synchronized (sharedLock) {
                    try {
                        if (queue.isEmpty() && !allPagesReady)
                            sharedLock.wait(waitTime);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteCheckedException("Query was interrupted: " + qry, e);
                    }
                }
            }
        }

        return it;
    }

    /** Loads next pages if there is no available. */
    protected abstract void loadPage();
}
