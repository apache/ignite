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

package org.apache.ignite.internal.processors.platform.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformFieldsQueryCursor;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformQueryCursor;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Native cache wrapper implementation.
 */
@SuppressWarnings({"unchecked", "UnusedDeclaration", "TryFinallyCanBeTryWithResources"})
public class PlatformCache extends PlatformAbstractTarget {
    /** */
    public static final int OP_CLEAR = 1;

    /** */
    public static final int OP_CLEAR_ALL = 2;

    /** */
    public static final int OP_CONTAINS_KEY = 3;

    /** */
    public static final int OP_CONTAINS_KEYS = 4;

    /** */
    public static final int OP_GET = 5;

    /** */
    public static final int OP_GET_ALL = 6;

    /** */
    public static final int OP_GET_AND_PUT = 7;

    /** */
    public static final int OP_GET_AND_PUT_IF_ABSENT = 8;

    /** */
    public static final int OP_GET_AND_REMOVE = 9;

    /** */
    public static final int OP_GET_AND_REPLACE = 10;

    /** */
    public static final int OP_GET_NAME = 11;

    /** */
    public static final int OP_INVOKE = 12;

    /** */
    public static final int OP_INVOKE_ALL = 13;

    /** */
    public static final int OP_IS_LOCAL_LOCKED = 14;

    /** */
    public static final int OP_LOAD_CACHE = 15;

    /** */
    public static final int OP_LOC_EVICT = 16;

    /** */
    public static final int OP_LOC_LOAD_CACHE = 17;

    /** */
    public static final int OP_LOC_PROMOTE = 18;

    /** */
    public static final int OP_LOCAL_CLEAR = 20;

    /** */
    public static final int OP_LOCAL_CLEAR_ALL = 21;

    /** */
    public static final int OP_LOCK = 22;

    /** */
    public static final int OP_LOCK_ALL = 23;

    /** */
    public static final int OP_METRICS = 24;

    /** */
    private static final int OP_PEEK = 25;

    /** */
    private static final int OP_PUT = 26;

    /** */
    private static final int OP_PUT_ALL = 27;

    /** */
    public static final int OP_PUT_IF_ABSENT = 28;

    /** */
    public static final int OP_QRY_CONTINUOUS = 29;

    /** */
    public static final int OP_QRY_SCAN = 30;

    /** */
    public static final int OP_QRY_SQL = 31;

    /** */
    public static final int OP_QRY_SQL_FIELDS = 32;

    /** */
    public static final int OP_QRY_TXT = 33;

    /** */
    public static final int OP_REMOVE_ALL = 34;

    /** */
    public static final int OP_REMOVE_BOOL = 35;

    /** */
    public static final int OP_REMOVE_OBJ = 36;

    /** */
    public static final int OP_REPLACE_2 = 37;

    /** */
    public static final int OP_REPLACE_3 = 38;

    /** Underlying JCache. */
    private final IgniteCacheProxy cache;

    /** Whether this cache is created with "keepPortable" flag on the other side. */
    private final boolean keepPortable;

    /** */
    private static final GetAllWriter WRITER_GET_ALL = new GetAllWriter();

    /** */
    private static final EntryProcessorInvokeWriter WRITER_INVOKE = new EntryProcessorInvokeWriter();

    /** */
    private static final EntryProcessorInvokeAllWriter WRITER_INVOKE_ALL = new EntryProcessorInvokeAllWriter();

    /** Map with currently active locks. */
    private final ConcurrentMap<Long, Lock> lockMap = GridConcurrentFactory.newMap();

    /** Lock ID sequence. */
    private static final AtomicLong LOCK_ID_GEN = new AtomicLong();

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param cache Underlying cache.
     * @param keepPortable Keep portable flag.
     */
    public PlatformCache(PlatformContext platformCtx, IgniteCache cache, boolean keepPortable) {
        super(platformCtx);

        this.cache = (IgniteCacheProxy)cache;
        this.keepPortable = keepPortable;
    }

    /**
     * Gets cache with "skip-store" flag set.
     *
     * @return Cache with "skip-store" flag set.
     */
    public PlatformCache withSkipStore() {
        if (cache.delegate().skipStore())
            return this;

        return new PlatformCache(platformCtx, cache.withSkipStore(), keepPortable);
    }

    /**
     * Gets cache with "keep portable" flag.
     *
     * @return Cache with "keep portable" flag set.
     */
    public PlatformCache withKeepPortable() {
        if (keepPortable)
            return this;

        return new PlatformCache(platformCtx, cache.withSkipStore(), true);
    }

    /**
     * Gets cache with provided expiry policy.
     *
     * @param create Create.
     * @param update Update.
     * @param access Access.
     * @return Cache.
     */
    public PlatformCache withExpiryPolicy(final long create, final long update, final long access) {
        IgniteCache cache0 = cache.withExpiryPolicy(new InteropExpiryPolicy(create, update, access));

        return new PlatformCache(platformCtx, cache0, keepPortable);
    }

    /**
     * Gets cache with asynchronous mode enabled.
     *
     * @return Cache with asynchronous mode enabled.
     */
    public PlatformCache withAsync() {
        if (cache.isAsync())
            return this;

        return new PlatformCache(platformCtx, (IgniteCache)cache.withAsync(), keepPortable);
    }

    /**
     * Gets cache with no-retries mode enabled.
     *
     * @return Cache with no-retries mode enabled.
     */
    public PlatformCache withNoRetries() {
        CacheOperationContext opCtx = cache.operationContext();

        if (opCtx != null && opCtx.noRetries())
            return this;

        return new PlatformCache(platformCtx, cache.withNoRetries(), keepPortable);
    }

    /** {@inheritDoc} */
    @Override protected long processInStreamOutLong(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_PUT:
                cache.put(reader.readObjectDetached(), reader.readObjectDetached());

                return TRUE;

            case OP_REMOVE_BOOL:
                return cache.remove(reader.readObjectDetached(), reader.readObjectDetached()) ? TRUE : FALSE;

            case OP_REMOVE_ALL:
                cache.removeAll(PlatformUtils.readSet(reader));

                return TRUE;

            case OP_PUT_ALL:
                cache.putAll(PlatformUtils.readMap(reader));

                return TRUE;

            case OP_LOC_EVICT:
                cache.localEvict(PlatformUtils.readCollection(reader));

                return TRUE;

            case OP_CONTAINS_KEY:
                return cache.containsKey(reader.readObjectDetached()) ? TRUE : FALSE;

            case OP_CONTAINS_KEYS:
                return cache.containsKeys(PlatformUtils.readSet(reader)) ? TRUE : FALSE;

            case OP_LOC_PROMOTE: {
                cache.localPromote(PlatformUtils.readSet(reader));

                break;
            }

            case OP_REPLACE_3:
                return cache.replace(reader.readObjectDetached(), reader.readObjectDetached(),
                    reader.readObjectDetached()) ? TRUE : FALSE;

            case OP_LOC_LOAD_CACHE:
                loadCache0(reader, true);

                break;

            case OP_LOAD_CACHE:
                loadCache0(reader, false);

                break;

            case OP_CLEAR:
                cache.clear(reader.readObjectDetached());

                break;

            case OP_CLEAR_ALL:
                cache.clearAll(PlatformUtils.readSet(reader));

                break;

            case OP_LOCAL_CLEAR:
                cache.localClear(reader.readObjectDetached());

                break;

            case OP_LOCAL_CLEAR_ALL:
                cache.localClearAll(PlatformUtils.readSet(reader));

                break;

            case OP_PUT_IF_ABSENT: {
                return cache.putIfAbsent(reader.readObjectDetached(), reader.readObjectDetached()) ? TRUE : FALSE;
            }

            case OP_REPLACE_2: {
                return cache.replace(reader.readObjectDetached(), reader.readObjectDetached()) ? TRUE : FALSE;
            }

            case OP_REMOVE_OBJ: {
                return cache.remove(reader.readObjectDetached()) ? TRUE : FALSE;
            }

            case OP_IS_LOCAL_LOCKED:
                return cache.isLocalLocked(reader.readObjectDetached(), reader.readBoolean()) ? TRUE : FALSE;

            default:
                return super.processInStreamOutLong(type, reader);
        }

        return TRUE;
    }

    /**
     * Loads cache via localLoadCache or loadCache.
     */
    private void loadCache0(PortableRawReaderEx reader, boolean loc) throws IgniteCheckedException {
        PlatformCacheEntryFilter filter = null;

        Object pred = reader.readObjectDetached();

        if (pred != null)
            filter = platformCtx.createCacheEntryFilter(pred, reader.readLong());

        Object[] args = reader.readObjectArray();

        if (loc)
            cache.localLoadCache(filter, args);
        else
            cache.loadCache(filter, args);
    }

    /** {@inheritDoc} */
    @Override protected Object processInStreamOutObject(int type, PortableRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_QRY_SQL:
                return runQuery(reader, readSqlQuery(reader));

            case OP_QRY_SQL_FIELDS:
                return runFieldsQuery(reader, readFieldsQuery(reader));

            case OP_QRY_TXT:
                return runQuery(reader, readTextQuery(reader));

            case OP_QRY_SCAN:
                return runQuery(reader, readScanQuery(reader));

            case OP_QRY_CONTINUOUS: {
                long ptr = reader.readLong();
                boolean loc = reader.readBoolean();
                boolean hasFilter = reader.readBoolean();
                Object filter = reader.readObjectDetached();
                int bufSize = reader.readInt();
                long timeInterval = reader.readLong();
                boolean autoUnsubscribe = reader.readBoolean();
                Query initQry = readInitialQuery(reader);

                PlatformContinuousQuery qry = platformCtx.createContinuousQuery(ptr, hasFilter, filter);

                qry.start(cache, loc, bufSize, timeInterval, autoUnsubscribe, initQry);

                return qry;
            }

            default:
                return super.processInStreamOutObject(type, reader);
        }
    }

    /**
     * Read arguments for SQL query.
     *
     * @param reader Reader.
     * @return Arguments.
     */
    @Nullable private Object[] readQueryArgs(PortableRawReaderEx reader) {
        int cnt = reader.readInt();

        if (cnt > 0) {
            Object[] args = new Object[cnt];

            for (int i = 0; i < cnt; i++)
                args[i] = reader.readObjectDetached();

            return args;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override protected void processOutStream(int type, PortableRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_NAME:
                writer.writeObject(cache.getName());

                break;

            case OP_METRICS:
                CacheMetrics metrics = cache.metrics();

                writer.writeLong(metrics.getCacheGets());
                writer.writeLong(metrics.getCachePuts());
                writer.writeLong(metrics.getCacheHits());
                writer.writeLong(metrics.getCacheMisses());
                writer.writeLong(metrics.getCacheTxCommits());
                writer.writeLong(metrics.getCacheTxRollbacks());
                writer.writeLong(metrics.getCacheEvictions());
                writer.writeLong(metrics.getCacheRemovals());
                writer.writeFloat(metrics.getAveragePutTime());
                writer.writeFloat(metrics.getAverageGetTime());
                writer.writeFloat(metrics.getAverageRemoveTime());
                writer.writeFloat(metrics.getAverageTxCommitTime());
                writer.writeFloat(metrics.getAverageTxRollbackTime());
                writer.writeString(metrics.name());
                writer.writeLong(metrics.getOverflowSize());
                writer.writeLong(metrics.getOffHeapEntriesCount());
                writer.writeLong(metrics.getOffHeapAllocatedSize());
                writer.writeInt(metrics.getSize());
                writer.writeInt(metrics.getKeySize());
                writer.writeBoolean(metrics.isEmpty());
                writer.writeInt(metrics.getDhtEvictQueueCurrentSize());
                writer.writeInt(metrics.getTxThreadMapSize());
                writer.writeInt(metrics.getTxXidMapSize());
                writer.writeInt(metrics.getTxCommitQueueSize());
                writer.writeInt(metrics.getTxPrepareQueueSize());
                writer.writeInt(metrics.getTxStartVersionCountsSize());
                writer.writeInt(metrics.getTxCommittedVersionsSize());
                writer.writeInt(metrics.getTxRolledbackVersionsSize());
                writer.writeInt(metrics.getTxDhtThreadMapSize());
                writer.writeInt(metrics.getTxDhtXidMapSize());
                writer.writeInt(metrics.getTxDhtCommitQueueSize());
                writer.writeInt(metrics.getTxDhtPrepareQueueSize());
                writer.writeInt(metrics.getTxDhtStartVersionCountsSize());
                writer.writeInt(metrics.getTxDhtCommittedVersionsSize());
                writer.writeInt(metrics.getTxDhtRolledbackVersionsSize());
                writer.writeBoolean(metrics.isWriteBehindEnabled());
                writer.writeInt(metrics.getWriteBehindFlushSize());
                writer.writeInt(metrics.getWriteBehindFlushThreadCount());
                writer.writeLong(metrics.getWriteBehindFlushFrequency());
                writer.writeInt(metrics.getWriteBehindStoreBatchSize());
                writer.writeInt(metrics.getWriteBehindTotalCriticalOverflowCount());
                writer.writeInt(metrics.getWriteBehindCriticalOverflowCount());
                writer.writeInt(metrics.getWriteBehindErrorRetryCount());
                writer.writeInt(metrics.getWriteBehindBufferSize());
                writer.writeString(metrics.getKeyType());
                writer.writeString(metrics.getValueType());
                writer.writeBoolean(metrics.isStoreByValue());
                writer.writeBoolean(metrics.isStatisticsEnabled());
                writer.writeBoolean(metrics.isManagementEnabled());
                writer.writeBoolean(metrics.isReadThrough());
                writer.writeBoolean(metrics.isWriteThrough());
                writer.writeFloat(metrics.getCacheHitPercentage());
                writer.writeFloat(metrics.getCacheMissPercentage());

                break;

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "ConstantConditions"})
    @Override protected void processInStreamOutStream(int type, PortableRawReaderEx reader, PortableRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_GET: {
                writer.writeObjectDetached(cache.get(reader.readObjectDetached()));

                break;
            }

            case OP_GET_AND_PUT: {
                writer.writeObjectDetached(cache.getAndPut(reader.readObjectDetached(), reader.readObjectDetached()));

                break;
            }

            case OP_GET_AND_REPLACE: {
                writer.writeObjectDetached(cache.getAndReplace(reader.readObjectDetached(),
                    reader.readObjectDetached()));

                break;
            }

            case OP_GET_AND_REMOVE: {
                writer.writeObjectDetached(cache.getAndRemove(reader.readObjectDetached()));

                break;
            }

            case OP_GET_AND_PUT_IF_ABSENT: {
                writer.writeObjectDetached(cache.getAndPutIfAbsent(reader.readObjectDetached(), reader.readObjectDetached()));

                break;
            }

            case OP_PEEK: {
                Object key = reader.readObjectDetached();

                CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes(reader.readInt());

                writer.writeObjectDetached(cache.localPeek(key, modes));

                break;
            }

            case OP_GET_ALL: {
                Set keys = PlatformUtils.readSet(reader);

                Map entries = cache.getAll(keys);

                PlatformUtils.writeNullableMap(writer, entries);

                break;
            }

            case OP_INVOKE: {
                Object key = reader.readObjectDetached();

                CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                try {
                    writer.writeObjectDetached(cache.invoke(key, proc));
                }
                catch (EntryProcessorException ex)
                {
                    if (ex.getCause() instanceof PlatformNativeException)
                        writer.writeObjectDetached(((PlatformNativeException)ex.getCause()).cause());
                    else
                        throw ex;
                }

                break;
            }

            case OP_INVOKE_ALL: {
                Set<Object> keys = PlatformUtils.readSet(reader);

                CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                writeInvokeAllResult(writer, cache.invokeAll(keys, proc));

                break;
            }

            case OP_LOCK:
                writer.writeLong(registerLock(cache.lock(reader.readObjectDetached())));

                break;

            case OP_LOCK_ALL:
                writer.writeLong(registerLock(cache.lockAll(PlatformUtils.readCollection(reader))));

                break;

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public Exception convertException(Exception e) {
        if (e instanceof CachePartialUpdateException)
            return new PlatformCachePartialUpdateException((CachePartialUpdateCheckedException)e.getCause(),
                platformCtx, keepPortable);

        if (e instanceof CachePartialUpdateCheckedException)
            return new PlatformCachePartialUpdateException((CachePartialUpdateCheckedException)e, platformCtx, keepPortable);

        if (e.getCause() instanceof EntryProcessorException)
            return (EntryProcessorException) e.getCause();

        return super.convertException(e);
    }

    /**
     * Writes the result of InvokeAll cache method.
     *
     * @param writer Writer.
     * @param results Results.
     */
    private static void writeInvokeAllResult(PortableRawWriterEx writer, Map<Object, EntryProcessorResult> results) {
        if (results == null) {
            writer.writeInt(-1);

            return;
        }

        writer.writeInt(results.size());

        for (Map.Entry<Object, EntryProcessorResult> entry : results.entrySet()) {
            writer.writeObjectDetached(entry.getKey());

            EntryProcessorResult procRes = entry.getValue();

            try {
                Object res = procRes.get();

                writer.writeBoolean(false);  // No exception

                writer.writeObjectDetached(res);
            }
            catch (Exception ex) {
                writer.writeBoolean(true);  // Exception

                writeError(writer, ex);
            }
        }
    }

    /**
     * Writes an error to the writer either as a native exception, or as a couple of strings.
     * @param writer Writer.
     * @param ex Exception.
     */
    private static void writeError(PortableRawWriterEx writer, Exception ex) {
        if (ex.getCause() instanceof PlatformNativeException)
            writer.writeObjectDetached(((PlatformNativeException)ex.getCause()).cause());
        else {
            writer.writeObjectDetached(ex.getClass().getName());
            writer.writeObjectDetached(ex.getMessage());
        }
    }

    /** <inheritDoc /> */
    @Override protected IgniteFuture currentFuture() throws IgniteCheckedException {
        return cache.future();
    }

    /** <inheritDoc /> */
    @Nullable @Override protected PlatformFutureUtils.Writer futureWriter(int opId) {
        if (opId == OP_GET_ALL)
            return WRITER_GET_ALL;

        if (opId == OP_INVOKE)
            return WRITER_INVOKE;

        if (opId == OP_INVOKE_ALL)
            return WRITER_INVOKE_ALL;

        return null;
    }

    /**
     * Clears the contents of the cache, without notifying listeners or
     * {@ignitelink javax.cache.integration.CacheWriter}s.
     *
     * @throws IllegalStateException if the cache is closed.
     * @throws javax.cache.CacheException if there is a problem during the clear
     */
    public void clear() throws IgniteCheckedException {
        cache.clear();
    }

    /**
     * Removes all entries.
     *
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public void removeAll() throws IgniteCheckedException {
        cache.removeAll();
    }

    /**
     * Read cache size.
     *
     * @param peekModes Encoded peek modes.
     * @param loc Local mode flag.
     * @return Size.
     */
    public int size(int peekModes, boolean loc) {
        CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes(peekModes);

        return loc ? cache.localSize(modes) :  cache.size(modes);
    }

    /**
     * Create cache iterator.
     *
     * @return Cache iterator.
     */
    public PlatformCacheIterator iterator() {
        Iterator<Cache.Entry> iter = cache.iterator();

        return new PlatformCacheIterator(platformCtx, iter);
    }

    /**
     * Create cache iterator over local entries.
     *
     * @param peekModes Peke modes.
     * @return Cache iterator.
     */
    public PlatformCacheIterator localIterator(int peekModes) {
        CachePeekMode[] peekModes0 = PlatformUtils.decodeCachePeekModes(peekModes);

        Iterator<Cache.Entry> iter = cache.localEntries(peekModes0).iterator();

        return new PlatformCacheIterator(platformCtx, iter);
    }

    /**
     * Enters a lock.
     *
     * @param id Lock id.
     */
    public void enterLock(long id) throws InterruptedException {
        lock(id).lockInterruptibly();
    }

    /**
     * Exits a lock.
     *
     * @param id Lock id.
     */
    public void exitLock(long id) {
        lock(id).unlock();
    }

    /**
     * Attempts to enter a lock.
     *
     * @param id Lock id.
     * @param timeout Timeout, in milliseconds. -1 for infinite timeout.
     */
    public boolean tryEnterLock(long id, long timeout) throws InterruptedException {
        return timeout == -1
            ? lock(id).tryLock()
            : lock(id).tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Rebalances the cache.
     *
     * @param futId Future id.
     */
    public void rebalance(long futId) {
        PlatformFutureUtils.listen(platformCtx, cache.rebalance().chain(new C1<IgniteFuture, Object>() {
            @Override public Object apply(IgniteFuture fut) {
                return null;
            }
        }), futId, PlatformFutureUtils.TYP_OBJ, this);
    }

    /**
     * Unregister lock.
     *
     * @param id Lock id.
     */
    public void closeLock(long id){
        Lock lock = lockMap.remove(id);

        assert lock != null : "Failed to unregister lock: " + id;
    }

    /**
     * Get lock by id.
     *
     * @param id Id.
     * @return Lock.
     */
    private Lock lock(long id) {
        Lock lock = lockMap.get(id);

        assert lock != null : "Lock not found for ID: " + id;

        return lock;
    }

    /**
     * Registers a lock in a map.
     *
     * @param lock Lock to register.
     * @return Registered lock id.
     */
    private long registerLock(Lock lock) {
        long id = LOCK_ID_GEN.incrementAndGet();

        lockMap.put(id, lock);

        return id;
    }

    /**
     * Runs specified query.
     */
    private PlatformQueryCursor runQuery(PortableRawReaderEx reader, Query qry) throws IgniteCheckedException {

        try {
            QueryCursorEx cursor = (QueryCursorEx) cache.query(qry);

            return new PlatformQueryCursor(platformCtx, cursor,
                qry.getPageSize() > 0 ? qry.getPageSize(): Query.DFLT_PAGE_SIZE);
        }
        catch (Exception err) {
            throw PlatformUtils.unwrapQueryException(err);
        }
    }

    /**
     * Runs specified fields query.
     */
    private PlatformFieldsQueryCursor runFieldsQuery(PortableRawReaderEx reader, Query qry)
        throws IgniteCheckedException {
        try {
            QueryCursorEx cursor = (QueryCursorEx) cache.query(qry);

            return new PlatformFieldsQueryCursor(platformCtx, cursor,
                qry.getPageSize() > 0 ? qry.getPageSize() : Query.DFLT_PAGE_SIZE);
        }
        catch (Exception err) {
            throw PlatformUtils.unwrapQueryException(err);
        }
    }

    /**
     * Reads the query of specified type.
     */
    private Query readInitialQuery(PortableRawReaderEx reader) throws IgniteCheckedException {
        int typ = reader.readInt();

        switch (typ) {
            case -1:
                return null;

            case OP_QRY_SCAN:
                return readScanQuery(reader);

            case OP_QRY_SQL:
                return readSqlQuery(reader);

            case OP_QRY_TXT:
                return readTextQuery(reader);
        }

        throw new IgniteCheckedException("Unsupported query type: " + typ);
    }

    /**
     * Reads sql query.
     */
    private Query readSqlQuery(PortableRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        String sql = reader.readString();
        String typ = reader.readString();
        final int pageSize = reader.readInt();

        Object[] args = readQueryArgs(reader);

        return new SqlQuery(typ, sql).setPageSize(pageSize).setArgs(args).setLocal(loc);
    }

    /**
     * Reads fields query.
     */
    private Query readFieldsQuery(PortableRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        String sql = reader.readString();
        final int pageSize = reader.readInt();

        Object[] args = readQueryArgs(reader);

        return new SqlFieldsQuery(sql).setPageSize(pageSize).setArgs(args).setLocal(loc);
    }

    /**
     * Reads text query.
     */
    private Query readTextQuery(PortableRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        String txt = reader.readString();
        String typ = reader.readString();
        final int pageSize = reader.readInt();

        return new TextQuery(typ, txt).setPageSize(pageSize).setLocal(loc);
    }

    /**
     * Reads scan query.
     */
    private Query readScanQuery(PortableRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        final int pageSize = reader.readInt();

        boolean hasPart = reader.readBoolean();

        Integer part = hasPart ? reader.readInt() : null;

        ScanQuery qry = new ScanQuery().setPageSize(pageSize);

        qry.setPartition(part);

        Object pred = reader.readObjectDetached();

        if (pred != null)
            qry.setFilter(platformCtx.createCacheEntryFilter(pred, reader.readLong()));

        qry.setLocal(loc);

        return qry;
    }

    /**
     * Writes error with EntryProcessorException cause.
     */
    private static class GetAllWriter implements PlatformFutureUtils.Writer {
        /** <inheritDoc /> */
        @Override public void write(PortableRawWriterEx writer, Object obj, Throwable err) {
            assert obj instanceof Map;

            PlatformUtils.writeNullableMap(writer, (Map) obj);
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return err == null;
        }
    }

    /**
     * Writes error with EntryProcessorException cause.
     */
    private static class EntryProcessorInvokeWriter implements PlatformFutureUtils.Writer {
        /** <inheritDoc /> */
        @Override public void write(PortableRawWriterEx writer, Object obj, Throwable err) {
            if (err == null) {
                writer.writeBoolean(false);  // No error.

                writer.writeObjectDetached(obj);
            }
            else {
                writer.writeBoolean(true);  // Error.

                writeError(writer, (Exception) err);
            }
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return true;
        }
    }

    /**
     * Writes results of InvokeAll method.
     */
    private static class EntryProcessorInvokeAllWriter implements PlatformFutureUtils.Writer {
        /** <inheritDoc /> */
        @Override public void write(PortableRawWriterEx writer, Object obj, Throwable err) {
            writeInvokeAllResult(writer, (Map)obj);
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return obj != null && err == null;
        }
    }

    /**
     * Interop expiry policy.
     */
    private static class InteropExpiryPolicy implements ExpiryPolicy {
        /** Duration: unchanged. */
        private static final long DUR_UNCHANGED = -2;

        /** Duration: eternal. */
        private static final long DUR_ETERNAL = -1;

        /** Duration: zero. */
        private static final long DUR_ZERO = 0;

        /** Expiry for create. */
        private final Duration create;

        /** Expiry for update. */
        private final Duration update;

        /** Expiry for access. */
        private final Duration access;

        /**
         * Constructor.
         *
         * @param create Expiry for create.
         * @param update Expiry for update.
         * @param access Expiry for access.
         */
        public InteropExpiryPolicy(long create, long update, long access) {
            this.create = convert(create);
            this.update = convert(update);
            this.access = convert(access);
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return create;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return update;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return access;
        }

        /**
         * Convert encoded duration to actual duration.
         *
         * @param dur Encoded duration.
         * @return Actual duration.
         */
        private static Duration convert(long dur) {
            if (dur == DUR_UNCHANGED)
                return null;
            else if (dur == DUR_ETERNAL)
                return Duration.ETERNAL;
            else if (dur == DUR_ZERO)
                return Duration.ZERO;
            else {
                assert dur > 0;

                return new Duration(TimeUnit.MILLISECONDS, dur);
            }
        }
    }
}