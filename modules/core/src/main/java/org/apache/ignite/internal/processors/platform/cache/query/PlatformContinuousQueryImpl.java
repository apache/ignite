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

package org.apache.ignite.internal.processors.platform.cache.query;

import java.io.ObjectStreamException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformJavaObjectFactoryProxy;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * Interop continuous query handle.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PlatformContinuousQueryImpl implements PlatformContinuousQuery {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    protected final PlatformContext platformCtx;

    /** Whether filter exists. */
    private final boolean hasFilter;

    /** Native filter in serialized form. If null, then filter is either not set, or this is local query. */
    protected final Object filter;

    /** Java filter. */
    protected final CacheEntryEventFilter javaFilter;

    /** Pointer to native counterpart; zero if closed. */
    private long ptr;

    /** Cursor to handle filter close. */
    private QueryCursor cursor;

    /** Lock for concurrency control. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Wrapped initial qry cursor. */
    private PlatformAbstractQueryCursor initialQryCur;

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

        javaFilter = getJavaFilter(filter, platformCtx.kernalContext());

        if (javaFilter != null && !(javaFilter instanceof CacheEntryEventSerializableFilter))
            throw new IgniteException("Java event filter must implement " +
                CacheEntryEventSerializableFilter.class.getName() + " interface: " + javaFilter.getClass().getName());
    }

    /**
     * Gets the Java filter if present.
     *
     * @param filter Filter object.
     * @param ctx Context.
     * @return Java filter or null.
     */
    private static CacheEntryEventFilter getJavaFilter(Object filter, GridKernalContext ctx) {
        if (filter instanceof BinaryObjectImpl) {
            BinaryObjectImpl bo = (BinaryObjectImpl)filter;

            if (bo.typeId() == GridBinaryMarshaller.PLATFORM_JAVA_OBJECT_FACTORY_PROXY) {
                PlatformJavaObjectFactoryProxy prx = bo.deserialize();

                return (CacheEntryEventFilter)prx.factory(ctx).create();
            }
        }

        return null;
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
    @Override public void start(IgniteCacheProxy cache, boolean loc, int bufSize, long timeInterval,
        boolean autoUnsubscribe, Query initialQry) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            try {
                ContinuousQuery qry = new ContinuousQuery();

                qry.setLocalListener(this);

                //noinspection deprecation
                qry.setRemoteFilter(this); // Filter must be set always for correct resource release.

                qry.setPageSize(bufSize);
                qry.setTimeInterval(timeInterval);
                qry.setAutoUnsubscribe(autoUnsubscribe);
                qry.setInitialQuery(initialQry);

                cursor = cache.query(qry.setLocal(loc));

                initialQryCur = getInitialQueryCursor(initialQry);
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
        if (javaFilter != null)
            return javaFilter.evaluate(evt);

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
    @Override public PlatformTarget getInitialQueryCursor() {
        return initialQryCur;
    }

    /**
     * Gets the initial query cursor.
     *
     * @param initialQry Initial query.
     * @return Cursor.
     */
    private PlatformAbstractQueryCursor getInitialQueryCursor(Query initialQry) {
        if (initialQry == null)
            return null;

        int batchSize = initialQry.getPageSize() > 0 ? initialQry.getPageSize() : Query.DFLT_PAGE_SIZE;

        if (initialQry instanceof SqlFieldsQuery)
            return new PlatformFieldsQueryCursor(platformCtx, new QueryCursorEx<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    return cursor.iterator();
                }

                @Override public List<List<?>> getAll() {
                    return cursor.getAll();
                }

                @Override public void close() {
                    // No-op: do not close whole continuous query when initial query cursor closes.
                }

                @Override public void getAll(Consumer<List<?>> clo) throws IgniteCheckedException {
                    for (List t : this)
                        clo.consume(t);
                }

                @Override public List<GridQueryFieldMetadata> fieldsMeta() {
                    return ((QueryCursorEx)cursor).fieldsMeta();
                }
            }, batchSize);

        return new PlatformQueryCursor(platformCtx, new QueryCursorEx<Cache.Entry>() {
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
        }, batchSize);
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
    protected Object writeReplace() throws ObjectStreamException {
        if (javaFilter != null)
            return javaFilter;

        return filter == null ? null : platformCtx.createContinuousQueryFilter(filter);
    }
}
