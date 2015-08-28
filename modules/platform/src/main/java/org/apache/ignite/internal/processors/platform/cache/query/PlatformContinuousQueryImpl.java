/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.platform.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.processors.platform.utils.*;
import org.apache.ignite.internal.processors.query.*;

import javax.cache.*;
import javax.cache.event.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Interop continuous query handle.
 */
public class PlatformContinuousQueryImpl implements PlatformContinuousQuery {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    protected final PlatformContext platformCtx;

    /** Whether filter exists. */
    private final boolean hasFilter;

    /** Native filter in serialized form. If null, then filter is either not set, or this is local query. */
    protected final Object filter;

    /** Pointer to native counterpart; zero if closed. */
    private long ptr;

    /** Cursor to handle filter close. */
    private QueryCursor cursor;

    /** Lock for concurrency control. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Wrapped initial qry cursor. */
    private PlatformQueryCursor initialQryCur;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param ptr Pointer to native counterpart.
     * @param hasFilter Whether filter exists.
     * @param filter Filter.
     */
    public PlatformContinuousQueryImpl(PlatformContext platformCtx, long ptr, boolean hasFilter, Object filter) {
        assert ptr != 0L;

        this.platformCtx = platformCtx;
        this.ptr = ptr;
        this.hasFilter = hasFilter;
        this.filter = filter;
    }

    /**
     * Start query execution.
     *
     * @param cache Cache.
     * @param loc Local flag.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto-unsubscribe flag.
     * @param initialQry Initial query.
     */
    @SuppressWarnings("unchecked")
    public void start(IgniteCacheProxy cache, boolean loc, int bufSize, long timeInterval, boolean autoUnsubscribe,
        Query initialQry) throws IgniteCheckedException {
        assert !loc || filter == null;

        lock.writeLock().lock();

        try {
            try {
                ContinuousQuery qry = new ContinuousQuery();

                qry.setLocalListener(this);
                qry.setRemoteFilter(this); // Filter must be set always for correct resource release.
                qry.setPageSize(bufSize);
                qry.setTimeInterval(timeInterval);
                qry.setAutoUnsubscribe(autoUnsubscribe);
                qry.setInitialQuery(initialQry);

                cursor = cache.query(qry.setLocal(loc));

                if (initialQry != null)
                    initialQryCur = new PlatformQueryCursor(platformCtx, new QueryCursorEx<Cache.Entry>() {
                        @Override public Iterator<Cache.Entry> iterator() {
                            return cursor.iterator();
                        }

                        @Override public List<Cache.Entry> getAll() {
                            return cursor.getAll();
                        }

                        @Override public void close() {
                            // No-op: do not close whole continuous query when initial query cursor closes.
                        }

                        @Override public void getAll(Consumer<Cache.Entry> clo) throws IgniteCheckedException {
                            for (Cache.Entry t : this)
                                clo.consume(t);
                        }

                        @Override public List<GridQueryFieldMetadata> fieldsMeta() {
                            return null;
                        }
                    }, initialQry.getPageSize() > 0 ? initialQry.getPageSize() : Query.DFLT_PAGE_SIZE);
            }
            catch (Exception e) {
                try
                {
                    close0();
                }
                catch (Exception ignored)
                {
                    // Ignore
                }

                throw PlatformUtils.unwrapQueryException(e);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onUpdated(Iterable evts) throws CacheEntryListenerException {
        lock.readLock().lock();

        try {
            if (ptr == 0)
                throw new CacheEntryListenerException("Failed to notify listener because it has been closed.");

            PlatformUtils.applyContinuousQueryEvents(platformCtx, ptr, evts);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent evt) throws CacheEntryListenerException {
        lock.readLock().lock();

        try {
            if (ptr == 0)
                throw new CacheEntryListenerException("Failed to evaluate the filter because it has been closed.");

            return !hasFilter || PlatformUtils.evaluateContinuousQueryEvent(platformCtx, ptr, evt);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onQueryUnregister() {
        close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        lock.writeLock().lock();

        try {
            close0();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"UnusedDeclaration", "unchecked"})
    @Override public PlatformTarget getInitialQueryCursor() {
        return initialQryCur;
    }

    /**
     * Internal close routine.
     */
    private void close0() {
        if (ptr != 0) {
            long ptr0 = ptr;

            ptr = 0;

            if (cursor != null)
                cursor.close();

            platformCtx.gateway().continuousQueryFilterRelease(ptr0);
        }
    }

    /**
     * Replacer for remote filter.
     *
     * @return Filter to be deployed on remote node.
     * @throws ObjectStreamException If failed.
     */
    Object writeReplace() throws ObjectStreamException {
        return filter == null ? null : platformCtx.createContinuousQueryFilter(filter);
    }
}
