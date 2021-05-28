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
 * Base abstract class for all Reducer descenders. It declares dependencies on cache query, and base logic for handling
 * remote pages.
 */
public abstract class AbstractCacheQueryReducer<R> implements CacheQueryReducer<R> {
    /** Query info. */
    private final GridCacheQueryAdapter qry;

    /** Flag shows whether all pages are ready. */
    protected volatile boolean allPagesReady;

    /** Timestamp when a query fails with timeout. */
    private final long timeoutTime;

    /** Lock shared between this reducer and future. */
    private final Object sharedLock;

    /**
     * @param fut Cache query future relates to this query. Future is done when last page is delivered to reducer.
     */
    AbstractCacheQueryReducer(GridCacheQueryFutureAdapter fut) {
        qry = fut.qry.query();
        timeoutTime = fut.endTime();
        // The only reason to use lock in 2 places is the deduplication mechanism.
        sharedLock = fut.lock;
    }

    /** {@inheritDoc} */
    @Override public Object sharedLock() {
        return sharedLock;
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        allPagesReady = true;
    }

    /**
     * Iterator over pages stream. Pages are stored in a queue. After polling a queue try load a new page instead of it.
     */
    class PageStream {
        /** Queue of data of results pages. */
        protected final Queue<Collection<R>> queue = new LinkedList<>();

        /** Iterator over current page. */
        private Iterator<R> iter;

        /** Add new query result page of data. */
        void addPage(Collection<R> data) {
            assert Thread.holdsLock(sharedLock);

            queue.add(data);
        }

        /**
         * @return {@code true} If this stream has next row, {@code false} otherwise.
         */
        public boolean hasNext() throws IgniteCheckedException {
            return streamIterator().hasNext();
        }

        /**
         * @return Next item from this stream.
         */
        public R next() throws IgniteCheckedException {
            return streamIterator().next();
        }

        /**
         * Returns iterator over a query result page. Load new pages after polling a queue.
         * Wait for new page if currently there is no any.
         *
         * @return Iterator over a query result page.
         * @throws IgniteCheckedException In case of error.
         */
        protected Iterator<R> streamIterator() throws IgniteCheckedException {
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
                    if (allPagesReady() && queue.peek() == null) {
                        if (it == null)
                            it = Collections.<R>emptyList().iterator();

                        break;
                    }
                }

                // Trigger loads next pages after polling queue.
                if (!allPagesReady())
                    loadPage();

                // Try load new page, and wait for that.
                if (c == null && !allPagesReady()) {
                    long timeout = qry.timeout();

                    long waitTime = timeout == 0 ? Long.MAX_VALUE : timeoutTime - U.currentTimeMillis();

                    if (waitTime <= 0) {
                        it = Collections.<R>emptyList().iterator();

                        break;
                    }

                    synchronized (sharedLock) {
                        try {
                            if (queue.isEmpty() && !allPagesReady())
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

        /**
         * @return {@code true} if this stream already loads all pages.
         */
        protected boolean allPagesReady() {
            return allPagesReady;
        }

        /** Clear all stored data in this stream. */
        protected void clear() {
            synchronized (sharedLock) {
                queue.clear();

                iter = null;
            }
        }
    }

    /** Loads next pages if there is no available. */
    protected abstract void loadPage();
}
