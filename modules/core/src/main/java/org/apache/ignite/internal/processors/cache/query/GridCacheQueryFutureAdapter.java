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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Query future adapter. Future marked as done after all result pages are ready. Note that completeness of this future
 * doesn't depend on whether all data are delievered to user or not. This class provides {@code Iterator} interface to
 * access result data. Handling of recieved pages is triggered by {@code Iterator} methods.
 * Reducing of result data (order, limit) is controlled by {@link CacheQueryReducer}.
 *
 * @param <R> Result type.
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

    /** */
    private int capacity;

    /** */
    private boolean limitDisabled;

    /** Set of received keys used to deduplicate query result set. */
    private final Collection<K> keys;

    /** */
    private int cnt;

    /** */
    private final IgniteUuid timeoutId = IgniteUuid.randomUuid();

    /** */
    private long endTime;

    /** */
    protected boolean loc;

    /** Lock on all future operations. */
    protected final Object lock = new Object();

    /** */
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

        long startTime = U.currentTimeMillis();

        long timeout = qry.query().timeout();
        capacity = query().query().limit();
        limitDisabled = capacity <= 0;

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

        reducer().onLastPage();

        return super.onDone(res, err);
    }

    /** {@inheritDoc} */
    @Override public R next() {
        try {
            if (!limitDisabled && cnt == capacity)
                return null;

            checkError();

            R next = null;

            if (reducer().hasNext()) {
                next = unmaskNull(reducer().next());

                if (!limitDisabled)
                    cnt++;
            }

            checkError();

            return next;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /** @return Cache query results reducer. */
    protected abstract CacheQueryReducer<R> reducer();

    /**
     * Waits for the first item to be received from remote node(s), if any.
     *
     * @throws IgniteCheckedException If query execution failed with an error.
     */
    public abstract void awaitFirstItem() throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If future is done with an error.
     */
    private void checkError() throws IgniteCheckedException {
        if (error() != null) {
            clear();

            throw U.cast(error());
        }
    }

    /**
     * @param evtNodeId Removed or failed node Id.
     */
    protected void onNodeLeft(UUID evtNodeId) {
        // No-op.
    }

    /**
     * @param col Query data collection.
     * @return If dedup flag is {@code true} deduplicated collection (considering keys), otherwise passed in collection
     * without any modifications.
     */
    private Collection<?> dedupIfRequired(Collection<?> col) {
        if (!qry.query().enableDedup())
            return col;

        Collection<Object> dedupCol = new ArrayList<>(col.size());

        synchronized (lock) {
            for (Object o : col)
                if (!(o instanceof Map.Entry) || keys.add(((Map.Entry<K, V>)o).getKey()))
                    dedupCol.add(o);
        }

        return dedupCol;
    }

    /**
     * Entrypoint for handling query result page from remote node.
     *
     * @param nodeId Sender node.
     * @param data Page data.
     * @param err Error (if was).
     * @param finished Whether it is the last page for sender node.
     */
    public void onPage(@Nullable UUID nodeId, @Nullable Collection<?> data, @Nullable Throwable err, boolean finished) {
        if (isCancelled())
            return;

        if (log.isDebugEnabled())
            log.debug(S.toString("Received query result page",
                "nodeId", nodeId, false,
                "data", data, true,
                "err", err, false,
                "finished", finished, false));

        try {
            if (err != null)
                synchronized (lock) {
                    reducer().addPage(nodeId, Collections.emptyList());

                    if (err instanceof IgniteCheckedException)
                        onDone(err);
                    else
                        onDone(new IgniteCheckedException(nodeId != null ?
                            S.toString("Failed to execute query on node",
                                "query", qry, true,
                                "nodeId", nodeId, false) :
                            S.toString("Failed to execute query locally",
                                "query", qry, true),
                            err));

                    onPage(nodeId, true);

                    lock.notifyAll();
                }
            else {
                if (data == null)
                    data = Collections.emptyList();

                data = dedupIfRequired(data);

                if (qry.query().type() == GridCacheQueryType.TEXT) {
                    ArrayList unwrapped = new ArrayList();

                    for (Object o: data) {
                        CacheEntryWithPayload e = (CacheEntryWithPayload) o;

                        Object uKey = CacheObjectUtils.unwrapBinary(
                            cctx.cacheObjectContext(), e.getKey(), qry.query().keepBinary(), true, null);

                        Object uVal = CacheObjectUtils.unwrapBinary(
                            cctx.cacheObjectContext(), e.getValue(), qry.query().keepBinary(), true, null);

                        if (uKey != e.getKey() || uVal != e.getValue())
                            unwrapped.add(new CacheEntryWithPayload<>(uKey, uVal, e.payload()));
                        else
                            unwrapped.add(o);
                    }

                    data = unwrapped;

                } else
                    data = cctx.unwrapBinariesIfNeeded((Collection<Object>)data, qry.query().keepBinary());

                synchronized (lock) {
                    reducer().addPage(nodeId, (Collection<R>) data);

                    if (onPage(nodeId, finished)) {
                        onDone(/* data */);

                        clear();
                    }

                    lock.notifyAll();
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
        synchronized (lock) {
            reducer().addPage(nodeId, Collections.emptyList());

            onPage(nodeId, true);

            onDone(e);

            lock.notifyAll();
        }
    }

    /**
     * @param col Collection.
     * @return Collection with masked {@code null} values.
     */
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
     * Callback performs additional actions after page handling.
     *
     * @param nodeId Sender node id.
     * @param last Whether page is last for sender node.
     * @return Is query finished or not.
     */
    protected abstract boolean onPage(@Nullable UUID nodeId, boolean last);

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
    @Override public boolean onCancelled() {
        reducer().onLastPage();

        return super.onCancelled();
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        try {
            reducer().onLastPage();

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
}
