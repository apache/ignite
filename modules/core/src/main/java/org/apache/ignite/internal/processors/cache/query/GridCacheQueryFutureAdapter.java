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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Query future adapter.
 *
 * @param <R> Result type.
 *
 */
public abstract class GridCacheQueryFutureAdapter<K, V, R> extends GridFutureAdapter<Collection<R>>
    implements CacheQueryFuture<R>, GridTimeoutObject {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** */
    private static final Object NULL = new Object();

    /** Cache context. */
    protected GridCacheContext<K, V> cctx;

    /** */
    protected final GridCacheQueryBean qry;

    /** Set of received keys used to deduplicate query result set. */
    private final Collection<K> keys;

    /** */
    private final Queue<Collection<R>> queue = new LinkedList<>();

    /** */
    private final Collection<Object> allCol = new LinkedList<>();

    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /** */
    private Iterator<R> iter;

    /** */
    protected final Object mux = new Object();

    /** */
    private IgniteUuid timeoutId = IgniteUuid.randomUuid();

    /** */
    private long startTime;

    /** */
    private long endTime;

    /** */
    protected boolean loc;

    /**
     *
     */
    protected GridCacheQueryFutureAdapter() {
        qry = null;
        keys = null;
    }

    /**
     * @param cctx Context.
     * @param qry Query.
     * @param loc Local query or not.
     */
    protected GridCacheQueryFutureAdapter(GridCacheContext<K, V> cctx, GridCacheQueryBean qry, boolean loc) {
        this.cctx = cctx;
        this.qry = qry;
        this.loc = loc;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheQueryFutureAdapter.class);

        startTime = U.currentTimeMillis();

        long timeout = qry.query().timeout();

        if (timeout > 0) {
            endTime = startTime + timeout;

            // Protect against overflow.
            if (endTime < 0)
                endTime = Long.MAX_VALUE;

            cctx.time().addTimeoutObject(this);
        }

        keys = qry.query().enableDedup() ? new HashSet<K>() : null;
    }

    /**
     * @return Query.
     */
    public GridCacheQueryBean query() {
        return qry;
    }

    /**
     * @return If fields query.
     */
    boolean fields() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Collection<R> res, Throwable err) {
        cctx.time().removeTimeoutObject(this);

        qry.query().onCompleted(res, err, startTime(), duration());

        return super.onDone(res, err);
    }

    /** {@inheritDoc} */
    @Override public R next() {
        try {
            R next = unmaskNull(internalIterator().next());

            cnt.decrementAndGet();

            return next;
        }
        catch (NoSuchElementException ignored) {
            return null;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /**
     * Waits for the first page to be received from remote node(s), if any.
     *
     * @throws IgniteCheckedException If query execution failed with an error.
     */
    public abstract void awaitFirstPage() throws IgniteCheckedException;

    /**
     * Returns next page for the query.
     *
     * @return Next page or {@code null} if no more pages available.
     * @throws IgniteCheckedException If fetch failed.
     */
    public Collection<R> nextPage() throws IgniteCheckedException {
        return nextPage(qry.query().timeout(), startTime);
    }

    /**
     * Returns next page for the query.
     *
     * @param timeout Timeout.
     * @return Next page or {@code null} if no more pages available.
     * @throws IgniteCheckedException If fetch failed.
     */
    public Collection<R> nextPage(long timeout) throws IgniteCheckedException {
        return nextPage(timeout, U.currentTimeMillis());
    }

    /**
     * Returns next page for the query.
     *
     * @param timeout Timeout.
     * @param startTime Timeout wait start time.
     * @return Next page or {@code null} if no more pages available.
     * @throws IgniteCheckedException If fetch failed.
     */
    private Collection<R> nextPage(long timeout, long startTime) throws IgniteCheckedException {
        Collection<R> res = null;

        while (res == null) {
            synchronized (mux) {
                res = queue.poll();
            }

            if (res == null) {
                if (!isDone()) {
                    loadPage();

                    long waitTime = timeout == 0 ? Long.MAX_VALUE : timeout - (U.currentTimeMillis() - startTime);

                    if (waitTime <= 0)
                        break;

                    synchronized (mux) {
                        try {
                            if (queue.isEmpty() && !isDone())
                                mux.wait(waitTime);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new IgniteCheckedException("Query was interrupted: " + qry, e);
                        }
                    }
                }
                else
                    break;
            }
        }

        checkError();

        return res;
    }

    /**
     * @throws IgniteCheckedException If future is done with an error.
     */
    private void checkError() throws IgniteCheckedException {
        if (error() != null) {
            clear();

            throw new IgniteCheckedException("Query execution failed: " + qry, error());
        }
    }

    /**
     * @return Iterator.
     * @throws IgniteCheckedException In case of error.
     */
    private Iterator<R> internalIterator() throws IgniteCheckedException {
        checkError();

        Iterator<R> it = null;

        while (it == null || !it.hasNext()) {
            Collection<R> c;

            synchronized (mux) {
                it = iter;

                if (it != null && it.hasNext())
                    break;

                c = queue.poll();

                if (c != null)
                    it = iter = c.iterator();

                if (isDone() && queue.peek() == null)
                    break;
            }

            if (c == null && !isDone()) {
                loadPage();

                long timeout = qry.query().timeout();

                long waitTime = timeout == 0 ? Long.MAX_VALUE : timeout - (U.currentTimeMillis() - startTime);

                if (waitTime <= 0) {
                    it = Collections.<R>emptyList().iterator();

                    break;
                }

                synchronized (mux) {
                    try {
                        if (queue.isEmpty() && !isDone())
                            mux.wait(waitTime);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteCheckedException("Query was interrupted: " + qry, e);
                    }
                }
            }
        }

        checkError();

        return it;
    }

    /**
     * @param evtNodeId Removed or failed node Id.
     */
    protected void onNodeLeft(UUID evtNodeId) {
        // No-op.
    }

    /**
     * @param col Collection.
     */
    @SuppressWarnings({"unchecked"})
    protected void enqueue(Collection<?> col) {
        assert Thread.holdsLock(mux);

        queue.add((Collection<R>)col);

        cnt.addAndGet(col.size());
    }

    /**
     * @param col Query data collection.
     * @return If dedup flag is {@code true} deduplicated collection (considering keys),
     *      otherwise passed in collection without any modifications.
     */
    @SuppressWarnings({"unchecked"})
    private Collection<?> dedupIfRequired(Collection<?> col) {
        if (!qry.query().enableDedup())
            return col;

        Collection<Object> dedupCol = new ArrayList<>(col.size());

        synchronized (mux) {
            for (Object o : col)
                if (!(o instanceof Map.Entry) || keys.add(((Map.Entry<K, V>)o).getKey()))
                    dedupCol.add(o);
        }

        return dedupCol;
    }

    /**
     * @param nodeId Sender node.
     * @param data Page data.
     * @param err Error (if was).
     * @param finished Finished or not.
     */
    @SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
    public void onPage(@Nullable UUID nodeId, @Nullable Collection<?> data, @Nullable Throwable err, boolean finished) {
        if (isCancelled())
            return;

        if (log.isDebugEnabled())
            log.debug("Received query result page [nodeId=" + nodeId + ", data=" + data +
                ", err=" + err + ", finished=" + finished + "]");

        try {
            if (err != null)
                synchronized (mux) {
                    enqueue(Collections.emptyList());

                    onDone(nodeId != null ?
                        new IgniteCheckedException("Failed to execute query on node [query=" + qry +
                            ", nodeId=" + nodeId + "]", err) :
                        new IgniteCheckedException("Failed to execute query locally: " + qry, err));

                    onPage(nodeId, true);

                    mux.notifyAll();
                }
            else {
                if (data == null)
                    data = Collections.emptyList();

                data = dedupIfRequired((Collection<Object>)data);

                data = cctx.unwrapBinariesIfNeeded((Collection<Object>)data, qry.query().keepBinary());

                synchronized (mux) {
                    enqueue(data);

                    if (qry.query().keepAll())
                        allCol.addAll(maskNulls((Collection<Object>)data));

                    if (onPage(nodeId, finished)) {
                        onDone((Collection<R>)(qry.query().keepAll() ? unmaskNulls(allCol) : data));

                        clear();
                    }

                    mux.notifyAll();
                }
            }
        }
        catch (Throwable e) {
            onPageError(nodeId, e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param e Error.
     */
    private void onPageError(@Nullable UUID nodeId, Throwable e) {
        synchronized (mux) {
            enqueue(Collections.emptyList());

            onPage(nodeId, true);

            onDone(e);

            mux.notifyAll();
        }
    }

    /**
     * @param col Collection.
     * @return Collection with masked {@code null} values.
     */
    @SuppressWarnings("unchecked")
    private Collection<Object> maskNulls(Collection<Object> col) {
        assert col != null;

        return F.viewReadOnly(col, new C1<Object, Object>() {
            @Override public Object apply(Object e) {
                return e != null ? e : NULL;
            }
        });
    }

    /**
     * @param col Collection.
     * @return Collection with unmasked {@code null} values.
     */
    @SuppressWarnings("unchecked")
    private Collection<Object> unmaskNulls(Collection<Object> col) {
        assert col != null;

        return F.viewReadOnly(col, new C1<Object, Object>() {
            @Override public Object apply(Object e) {
                return e != NULL ? e : null;
            }
        });
    }

    /**
     * @param obj Object.
     * @return Unmasked object.
     */
    private R unmaskNull(R obj) {
        return obj != NULL ? obj : null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get() throws IgniteCheckedException {
        if (!isDone())
            loadAllPages();

        return super.get();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        if (!isDone())
            loadAllPages();

        return super.get(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getUninterruptibly() throws IgniteCheckedException {
        if (!isDone())
            loadAllPages();

        return super.getUninterruptibly();
    }

    /**
     * @param nodeId Sender node id.
     * @param last Whether page is last.
     * @return Is query finished or not.
     */
    protected abstract boolean onPage(UUID nodeId, boolean last);

    /**
     * Loads next page.
     */
    protected abstract void loadPage();

    /**
     * Loads all left pages.
     *
     * @throws IgniteInterruptedCheckedException If thread is interrupted.
     */
    protected abstract void loadAllPages() throws IgniteInterruptedCheckedException;

    /**
     * Clears future.
     */
    void clear() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        if (onCancelled()) {
            cancelQuery();

            return true;
        }
        else
            return false;
    }

    /**
     * @throws IgniteCheckedException In case of error.
     */
    protected abstract void cancelQuery() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return timeoutId;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        try {
            cancelQuery();

            onDone(new IgniteFutureTimeoutCheckedException("Query timed out."));
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cancel();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryFutureAdapter.class, this);
    }

    /** */
    public void printMemoryStats() {
        X.println(">>> Query future memory statistics.");
        X.println(">>>  queueSize: " + queue.size());
        X.println(">>>  allCollSize: " + allCol.size());
        X.println(">>>  keysSize: " + keys.size());
        X.println(">>>  cnt: " + cnt);
    }
}
