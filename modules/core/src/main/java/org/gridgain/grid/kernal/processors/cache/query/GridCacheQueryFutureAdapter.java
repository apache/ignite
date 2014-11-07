/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Query future adapter.
 *
 * @param <R> Result type.
 *
 */
public abstract class GridCacheQueryFutureAdapter<K, V, R> extends GridFutureAdapter<Collection<R>>
    implements GridCacheQueryFuture<R>, GridTimeoutObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** */
    private static final Object NULL = new Object();

    /** Cache context. */
    protected GridCacheContext<K, V> cctx;

    /** Logger. */
    protected GridLogger log;

    /** */
    protected final GridCacheQueryBean qry;

    /** Set of received keys used to deduplicate query result set. */
    private final Collection<K> keys = new HashSet<>();

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
    private GridUuid timeoutId = GridUuid.randomUuid();

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
    }

    /**
     * @param cctx Context.
     * @param qry Query.
     * @param loc Local query or not.
     */
    protected GridCacheQueryFutureAdapter(GridCacheContext<K, V> cctx, GridCacheQueryBean qry, boolean loc) {
        super(cctx.kernalContext());

        this.cctx = cctx;
        this.qry = qry;
        this.loc = loc;

        log = U.logger(ctx, logRef, GridCacheQueryFutureAdapter.class);

        startTime = U.currentTimeMillis();

        long timeout = qry.query().timeout();

        if (timeout > 0) {
            endTime = startTime + timeout;

            // Protect against overflow.
            if (endTime < 0)
                endTime = Long.MAX_VALUE;

            cctx.time().addTimeoutObject(this);
        }
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

        qry.query().onExecuted(res, err, startTime(), duration());

        return super.onDone(res, err);
    }

    /** {@inheritDoc} */
    @Override public int available() {
        return cnt.get();
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * Returns next page for the query.
     *
     * @return Next page or {@code null} if no more pages available.
     * @throws GridException If fetch failed.
     */
    public Collection<R> nextPage() throws GridException {
        Collection<R> res = null;

        while (res == null) {
            synchronized (mux) {
                res = queue.poll();
            }

            if (res == null) {
                if (!isDone()) {
                    loadPage();

                    long timeout = qry.query().timeout();

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

                            throw new GridException("Query was interrupted: " + qry, e);
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
     * @throws GridException If future is done with an error.
     */
    private void checkError() throws GridException {
        if (error() != null) {
            clear();

            throw new GridException("Query execution failed: " + qry, error());
        }
    }

    /**
     * @return Iterator.
     * @throws GridException In case of error.
     */
    private Iterator<R> internalIterator() throws GridException {
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

                        throw new GridException("Query was interrupted: " + qry, e);
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

        Collection<Object> dedupCol = new LinkedList<>();

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

                    onPage(nodeId, true);

                    onDone(nodeId != null ?
                        new GridException("Failed to execute query on node [query=" + qry +
                            ", nodeId=" + nodeId + "]", err) :
                        new GridException("Failed to execute query locally: " + qry, err));

                    mux.notifyAll();
                }
            else {
                if (data == null)
                    data = Collections.emptyList();

                data = dedupIfRequired((Collection<Object>)data);

                data = cctx.unwrapPortablesIfNeeded((Collection<Object>)data, qry.query().keepPortable());

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
        catch (GridException e) {
            synchronized (mux) {
                enqueue(Collections.emptyList());

                onPage(nodeId, true);

                onDone(e);

                mux.notifyAll();
            }
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
    @Override public Collection<R> get() throws GridException {
        if (!isDone())
            loadAllPages();

        return super.get();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get(long timeout, TimeUnit unit) throws GridException {
        if (!isDone())
            loadAllPages();

        return super.get(timeout, unit);
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
     * @throws GridInterruptedException If thread is interrupted.
     */
    protected abstract void loadAllPages() throws GridInterruptedException;

    /**
     * Clears future.
     */
    void clear() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        if (onCancelled()) {
            cancelQuery();

            return true;
        }
        else
            return false;
    }

    /**
     * @throws GridException In case of error.
     */
    protected abstract void cancelQuery() throws GridException;

    /** {@inheritDoc} */
    @Override public GridUuid timeoutId() {
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

            onDone(new GridFutureTimeoutException("Query timed out."));
        }
        catch (GridException e) {
            onDone(e);
        }
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
