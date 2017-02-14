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
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryProxy;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformFieldsQueryCursor;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformQueryCursor;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenable;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformWriterClosure;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CompletionListener;
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
@SuppressWarnings({"unchecked", "UnusedDeclaration", "TryFinallyCanBeTryWithResources", "TypeMayBeWeakened", "WeakerAccess"})
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
    public static final int OP_LOCAL_METRICS = 24;

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

    /** */
    public static final int OP_GET_CONFIG = 39;

    /** */
    public static final int OP_LOAD_ALL = 40;

    /** */
    public static final int OP_CLEAR_CACHE = 41;

    /** */
    public static final int OP_WITH_ASYNC = 42;

    /** */
    public static final int OP_REMOVE_ALL2 = 43;

    /** */
    public static final int OP_WITH_KEEP_BINARY = 44;

    /** */
    public static final int OP_WITH_EXPIRY_POLICY = 45;

    /** */
    public static final int OP_WITH_NO_RETRIES = 46;

    /** */
    public static final int OP_WITH_SKIP_STORE = 47;

    /** */
    public static final int OP_SIZE = 48;

    /** */
    public static final int OP_ITERATOR = 49;

    /** */
    public static final int OP_LOC_ITERATOR = 50;

    /** */
    public static final int OP_ENTER_LOCK = 51;

    /** */
    public static final int OP_EXIT_LOCK = 52;

    /** */
    public static final int OP_TRY_ENTER_LOCK = 53;

    /** */
    public static final int OP_CLOSE_LOCK = 54;

    /** */
    public static final int OP_REBALANCE = 55;

    /** */
    public static final int OP_SIZE_LOC = 56;

    /** */
    public static final int OP_PUT_ASYNC = 57;

    /** */
    public static final int OP_CLEAR_CACHE_ASYNC = 58;

    /** */
    public static final int OP_CLEAR_ALL_ASYNC = 59;

    /** */
    public static final int OP_REMOVE_ALL2_ASYNC = 60;

    /** */
    public static final int OP_SIZE_ASYNC = 61;

    /** */
    public static final int OP_CLEAR_ASYNC = 62;

    /** */
    public static final int OP_LOAD_CACHE_ASYNC = 63;

    /** */
    public static final int OP_LOC_LOAD_CACHE_ASYNC = 64;

    /** */
    public static final int OP_PUT_ALL_ASYNC = 65;

    /** */
    public static final int OP_REMOVE_ALL_ASYNC = 66;

    /** */
    public static final int OP_GET_ASYNC = 67;

    /** */
    public static final int OP_CONTAINS_KEY_ASYNC = 68;

    /** */
    public static final int OP_CONTAINS_KEYS_ASYNC = 69;

    /** */
    public static final int OP_REMOVE_BOOL_ASYNC = 70;

    /** */
    public static final int OP_REMOVE_OBJ_ASYNC = 71;

    /** */
    public static final int OP_GET_ALL_ASYNC = 72;

    /** */
    public static final int OP_GET_AND_PUT_ASYNC = 73;

    /** */
    public static final int OP_GET_AND_PUT_IF_ABSENT_ASYNC = 74;

    /** */
    public static final int OP_GET_AND_REMOVE_ASYNC = 75;

    /** */
    public static final int OP_GET_AND_REPLACE_ASYNC = 76;

    /** */
    public static final int OP_REPLACE_2_ASYNC = 77;

    /** */
    public static final int OP_REPLACE_3_ASYNC = 78;

    /** */
    public static final int OP_INVOKE_ASYNC = 79;

    /** */
    public static final int OP_INVOKE_ALL_ASYNC = 80;

    /** */
    public static final int OP_PUT_IF_ABSENT_ASYNC = 81;

    /** */
    public static final int OP_EXTENSION = 82;

    /** */
    public static final int OP_GLOBAL_METRICS = 83;

    /** Underlying JCache in binary mode. */
    private final IgniteCacheProxy cache;

    /** Initial JCache (not in binary mode). */
    private final IgniteCache rawCache;

    /** Underlying JCache in async mode. */
    private final IgniteCache cacheAsync;

    /** Whether this cache is created with "keepBinary" flag on the other side. */
    private final boolean keepBinary;

    /** */
    private static final PlatformFutureUtils.Writer WRITER_GET_ALL = new GetAllWriter();

    /** */
    private static final PlatformFutureUtils.Writer WRITER_INVOKE = new EntryProcessorInvokeWriter();

    /** */
    private static final PlatformFutureUtils.Writer WRITER_INVOKE_ALL = new EntryProcessorInvokeAllWriter();

    /** Map with currently active locks. */
    private final ConcurrentMap<Long, Lock> lockMap = GridConcurrentFactory.newMap();

    /** Lock ID sequence. */
    private static final AtomicLong LOCK_ID_GEN = new AtomicLong();

    /** Extensions. */
    private final PlatformCacheExtension[] exts;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param cache Underlying cache.
     * @param keepBinary Keep binary flag.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public PlatformCache(PlatformContext platformCtx, IgniteCache cache, boolean keepBinary) {
        this(platformCtx, cache, keepBinary, new PlatformCacheExtension[0]);
    }

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param cache Underlying cache.
     * @param keepBinary Keep binary flag.
     * @param exts Extensions.
     */
    public PlatformCache(PlatformContext platformCtx, IgniteCache cache, boolean keepBinary,
        PlatformCacheExtension[] exts) {
        super(platformCtx);

        assert cache != null;
        assert exts != null;

        rawCache = cache;
        IgniteCache binCache = cache.withKeepBinary();
        cacheAsync = binCache.withAsync();
        this.cache = (IgniteCacheProxy)binCache;
        this.keepBinary = keepBinary;
        this.exts = exts;
    }

    /**
     * @return Raw cache.
     */
    public IgniteCache rawCache() {
        return rawCache;
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem)
        throws IgniteCheckedException {
        try {
            switch (type) {
                case OP_PUT:
                    cache.put(reader.readObjectDetached(), reader.readObjectDetached());

                    return TRUE;

                case OP_GET:
                    return writeResult(mem, cache.get(reader.readObjectDetached()));

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

                    return TRUE;
                }

                case OP_REPLACE_3:
                    return cache.replace(reader.readObjectDetached(), reader.readObjectDetached(),
                        reader.readObjectDetached()) ? TRUE : FALSE;

                case OP_LOC_LOAD_CACHE:
                    loadCache0(reader, true, cache);

                    return TRUE;

                case OP_LOAD_CACHE:
                    loadCache0(reader, false, cache);

                    return TRUE;

                case OP_CLEAR:
                    cache.clear(reader.readObjectDetached());

                    return TRUE;

                case OP_CLEAR_ALL:
                    cache.clearAll(PlatformUtils.readSet(reader));

                    return TRUE;

                case OP_LOCAL_CLEAR:
                    cache.localClear(reader.readObjectDetached());

                    return TRUE;

                case OP_LOCAL_CLEAR_ALL:
                    cache.localClearAll(PlatformUtils.readSet(reader));

                    return TRUE;

                case OP_PUT_IF_ABSENT:
                    return cache.putIfAbsent(reader.readObjectDetached(), reader.readObjectDetached()) ? TRUE : FALSE;

                case OP_REPLACE_2:
                    return cache.replace(reader.readObjectDetached(), reader.readObjectDetached()) ? TRUE : FALSE;

                case OP_REMOVE_OBJ:
                    return cache.remove(reader.readObjectDetached()) ? TRUE : FALSE;

                case OP_IS_LOCAL_LOCKED:
                    return cache.isLocalLocked(reader.readObjectDetached(), reader.readBoolean()) ? TRUE : FALSE;

                case OP_LOAD_ALL: {
                    boolean replaceExisting = reader.readBoolean();
                    Set<Object> keys = PlatformUtils.readSet(reader);

                    long futId = reader.readLong();
                    int futTyp = reader.readInt();

                    CompletionListenable fut = new CompletionListenable();

                    PlatformFutureUtils.listen(platformCtx, fut, futId, futTyp, null, this);

                    cache.loadAll(keys, replaceExisting, fut);

                    return TRUE;
                }

                case OP_GET_AND_PUT:
                    return writeResult(mem, cache.getAndPut(reader.readObjectDetached(), reader.readObjectDetached()));

                case OP_GET_AND_REPLACE:
                    return writeResult(mem, cache.getAndReplace(reader.readObjectDetached(), reader.readObjectDetached()));

                case OP_GET_AND_REMOVE:
                    return writeResult(mem, cache.getAndRemove(reader.readObjectDetached()));

                case OP_GET_AND_PUT_IF_ABSENT:
                    return writeResult(mem, cache.getAndPutIfAbsent(reader.readObjectDetached(), reader.readObjectDetached()));

                case OP_PEEK: {
                    Object key = reader.readObjectDetached();

                    CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes(reader.readInt());

                    return writeResult(mem, cache.localPeek(key, modes));
                }

                case OP_TRY_ENTER_LOCK: {
                    try {
                        long id = reader.readLong();
                        long timeout = reader.readLong();

                        boolean res = timeout == -1
                            ? lock(id).tryLock()
                            : lock(id).tryLock(timeout, TimeUnit.MILLISECONDS);

                        return res ? TRUE : FALSE;
                    }
                    catch (InterruptedException e) {
                        throw new IgniteCheckedException(e);
                    }
                }

                case OP_GET_ALL: {
                    Set keys = PlatformUtils.readSet(reader);

                    Map entries = cache.getAll(keys);

                    return writeResult(mem, entries, new PlatformWriterClosure<Map>() {
                        @Override public void write(BinaryRawWriterEx writer, Map val) {
                            PlatformUtils.writeNullableMap(writer, val);
                        }
                    });
                }


                case OP_PUT_ASYNC: {
                    cacheAsync.put(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);
                }

                case OP_CLEAR_CACHE_ASYNC: {
                    cacheAsync.clear();

                    return readAndListenFuture(reader);
                }

                case OP_CLEAR_ALL_ASYNC: {
                    cacheAsync.clearAll(PlatformUtils.readSet(reader));

                    return readAndListenFuture(reader);
                }

                case OP_REMOVE_ALL2_ASYNC: {
                    cacheAsync.removeAll();

                    return readAndListenFuture(reader);
                }

                case OP_SIZE_ASYNC: {
                    CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes(reader.readInt());

                    cacheAsync.size(modes);

                    return readAndListenFuture(reader);
                }

                case OP_CLEAR_ASYNC: {
                    cacheAsync.clear(reader.readObjectDetached());

                    return readAndListenFuture(reader);
                }

                case OP_LOAD_CACHE_ASYNC: {
                    loadCache0(reader, false, cacheAsync);

                    return readAndListenFuture(reader);
                }

                case OP_LOC_LOAD_CACHE_ASYNC: {
                    loadCache0(reader, true, cacheAsync);

                    return readAndListenFuture(reader);
                }

                case OP_PUT_ALL_ASYNC:
                    cacheAsync.putAll(PlatformUtils.readMap(reader));

                    return readAndListenFuture(reader);

                case OP_REMOVE_ALL_ASYNC:
                    cacheAsync.removeAll(PlatformUtils.readSet(reader));

                    return readAndListenFuture(reader);

                case OP_REBALANCE:
                    readAndListenFuture(reader, cache.rebalance());

                    return TRUE;

                case OP_GET_ASYNC:
                    cacheAsync.get(reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_CONTAINS_KEY_ASYNC:
                    cacheAsync.containsKey(reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_CONTAINS_KEYS_ASYNC:
                    cacheAsync.containsKeys(PlatformUtils.readSet(reader));

                    return readAndListenFuture(reader);

                case OP_REMOVE_OBJ_ASYNC:
                    cacheAsync.remove(reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_REMOVE_BOOL_ASYNC:
                    cacheAsync.remove(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_GET_ALL_ASYNC: {
                    Set keys = PlatformUtils.readSet(reader);

                    cacheAsync.getAll(keys);

                    readAndListenFuture(reader, cacheAsync.future(), WRITER_GET_ALL);

                    return TRUE;
                }

                case OP_GET_AND_PUT_ASYNC:
                    cacheAsync.getAndPut(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_GET_AND_PUT_IF_ABSENT_ASYNC:
                    cacheAsync.getAndPutIfAbsent(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_GET_AND_REMOVE_ASYNC:
                    cacheAsync.getAndRemove(reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_GET_AND_REPLACE_ASYNC:
                    cacheAsync.getAndReplace(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_REPLACE_2_ASYNC:
                    cacheAsync.replace(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_REPLACE_3_ASYNC:
                    cacheAsync.replace(reader.readObjectDetached(), reader.readObjectDetached(),
                        reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_INVOKE_ASYNC: {
                    Object key = reader.readObjectDetached();

                    CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                    cacheAsync.invoke(key, proc);

                    readAndListenFuture(reader, cacheAsync.future(), WRITER_INVOKE);

                    return TRUE;
                }

                case OP_INVOKE_ALL_ASYNC: {
                    Set<Object> keys = PlatformUtils.readSet(reader);

                    CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                    cacheAsync.invokeAll(keys, proc);

                    readAndListenFuture(reader, cacheAsync.future(), WRITER_INVOKE_ALL);

                    return TRUE;
                }

                case OP_PUT_IF_ABSENT_ASYNC:
                    cacheAsync.putIfAbsent(reader.readObjectDetached(), reader.readObjectDetached());

                    return readAndListenFuture(reader);

                case OP_INVOKE: {
                    Object key = reader.readObjectDetached();

                    CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                    return writeResult(mem, cache.invoke(key, proc));
                }

                case OP_INVOKE_ALL: {
                    Set<Object> keys = PlatformUtils.readSet(reader);

                    CacheEntryProcessor proc = platformCtx.createCacheEntryProcessor(reader.readObjectDetached(), 0);

                    Map results = cache.invokeAll(keys, proc);

                    return writeResult(mem, results, new PlatformWriterClosure<Map>() {
                        @Override public void write(BinaryRawWriterEx writer, Map val) {
                            writeInvokeAllResult(writer, val);
                        }
                    });
                }

                case OP_LOCK: {
                    long id = registerLock(cache.lock(reader.readObjectDetached()));

                    return writeResult(mem, id, new PlatformWriterClosure<Long>() {
                        @Override public void write(BinaryRawWriterEx writer, Long val) {
                            writer.writeLong(val);
                        }
                    });
                }

                case OP_LOCK_ALL: {
                    long id = registerLock(cache.lockAll(PlatformUtils.readCollection(reader)));

                    return writeResult(mem, id, new PlatformWriterClosure<Long>() {
                        @Override public void write(BinaryRawWriterEx writer, Long val) {
                            writer.writeLong(val);
                        }
                    });
                }

                case OP_EXTENSION:
                    PlatformCacheExtension ext = extension(reader.readInt());

                    return ext.processInOutStreamLong(this, reader.readInt(), reader, mem);
            }
        }
        catch (Exception e) {
            PlatformOutputStream out = mem.output();
            BinaryRawWriterEx writer = platformCtx.writer(out);

            Exception err = convertException(e);

            PlatformUtils.writeError(err, writer);
            PlatformUtils.writeErrorData(err, writer);

            out.synchronize();

            return ERROR;
        }

        return super.processInStreamOutLong(type, reader, mem);
    }

    /**
     * Writes the result to reused stream, if any.
     */
    public long writeResult(PlatformMemory mem, Object obj) {
        return writeResult(mem, obj, null);
    }

    /**
     * Writes the result to reused stream, if any.
     */
    public long writeResult(PlatformMemory mem, Object obj, PlatformWriterClosure clo) {
        if (obj == null)
            return FALSE;

        PlatformOutputStream out = mem.output();
        BinaryRawWriterEx writer = platformCtx.writer(out);

        if (clo == null)
            writer.writeObjectDetached(obj);
        else
            clo.write(writer, obj);

        out.synchronize();
        return TRUE;
    }

    /**
     * Loads cache via localLoadCache or loadCache.
     */
    private void loadCache0(BinaryRawReaderEx reader, boolean loc, IgniteCache cache) {
        PlatformCacheEntryFilter filter = null;

        Object pred = reader.readObjectDetached();

        if (pred != null)
            filter = platformCtx.createCacheEntryFilter(pred, 0);

        Object[] args = null;

        int argCnt = reader.readInt();

        if (argCnt > 0) {
            args = new Object[argCnt];

            for (int i = 0; i < argCnt; i++)
                args[i] = reader.readObjectDetached();
        }

        if (loc)
            cache.localLoadCache(filter, args);
        else
            cache.loadCache(filter, args);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_QRY_SQL:
                return runQuery(readSqlQuery(reader));

            case OP_QRY_SQL_FIELDS:
                return runFieldsQuery(readFieldsQuery(reader));

            case OP_QRY_TXT:
                return runQuery(readTextQuery(reader));

            case OP_QRY_SCAN:
                return runQuery(readScanQuery(reader));

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

                return new PlatformContinuousQueryProxy(platformCtx, qry);
            }

            case OP_WITH_EXPIRY_POLICY: {
                long create = reader.readLong();
                long update = reader.readLong();
                long access = reader.readLong();

                IgniteCache cache0 = rawCache.withExpiryPolicy(new PlatformExpiryPolicy(create, update, access));

                return copy(cache0, keepBinary);
            }

            case OP_LOC_ITERATOR: {
                int peekModes = reader.readInt();

                CachePeekMode[] peekModes0 = PlatformUtils.decodeCachePeekModes(peekModes);

                Iterator<Cache.Entry> iter = cache.localEntries(peekModes0).iterator();

                return new PlatformCacheIterator(platformCtx, iter);
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
    @Nullable private Object[] readQueryArgs(BinaryRawReaderEx reader) {
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
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_NAME:
                writer.writeObject(cache.getName());

                break;

            case OP_LOCAL_METRICS: {
                CacheMetrics metrics = cache.localMetrics();

                writeCacheMetrics(writer, metrics);

                break;
            }

            case OP_GLOBAL_METRICS: {
                CacheMetrics metrics = cache.metrics();

                writeCacheMetrics(writer, metrics);

                break;
            }

            case OP_GET_CONFIG:
                CacheConfiguration ccfg = ((IgniteCache<Object, Object>)cache).
                        getConfiguration(CacheConfiguration.class);

                PlatformConfigurationUtils.writeCacheConfiguration(writer, ccfg);

                break;

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_ASYNC: {
                if (cache.isAsync())
                    return this;

                return copy(rawCache.withAsync(), keepBinary);
            }

            case OP_WITH_KEEP_BINARY: {
                if (keepBinary)
                    return this;

                return copy(rawCache.withKeepBinary(), true);
            }

            case OP_WITH_NO_RETRIES: {
                CacheOperationContext opCtx = cache.operationContext();

                if (opCtx != null && opCtx.noRetries())
                    return this;

                return copy(rawCache.withNoRetries(), keepBinary);
            }

            case OP_WITH_SKIP_STORE: {
                if (cache.delegate().skipStore())
                    return this;

                return copy(rawCache.withSkipStore(), keepBinary);
            }

            case OP_ITERATOR: {
                Iterator<Cache.Entry> iter = cache.iterator();

                return new PlatformCacheIterator(platformCtx, iter);
            }
        }

        return super.processOutObject(type);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_SIZE: {
                CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes((int)val);

                return cache.size(modes);
            }

            case OP_SIZE_LOC: {
                CachePeekMode[] modes = PlatformUtils.decodeCachePeekModes((int)val);

                return cache.localSize(modes);
            }

            case OP_ENTER_LOCK: {
                try {
                    lock(val).lockInterruptibly();

                    return TRUE;
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException("Failed to enter cache lock.", e);
                }
            }

            case OP_EXIT_LOCK: {
                lock(val).unlock();

                return TRUE;
            }

            case OP_CLOSE_LOCK: {
                Lock lock = lockMap.remove(val);

                assert lock != null : "Failed to unregister lock: " + val;

                return TRUE;
            }

            case OP_REBALANCE: {
                PlatformFutureUtils.listen(platformCtx, cache.rebalance().chain(new C1<IgniteFuture, Object>() {
                    @Override public Object apply(IgniteFuture fut) {
                        return null;
                    }
                }), val, PlatformFutureUtils.TYP_OBJ, this);

                return TRUE;
            }

            case OP_CLEAR_CACHE:
                cache.clear();

                return TRUE;

            case OP_REMOVE_ALL2:
                cache.removeAll();

                return TRUE;
        }
        return super.processInLongOutLong(type, val);
    }

    /** {@inheritDoc} */
    @Override public Exception convertException(Exception e) {
        if (e instanceof CachePartialUpdateException)
            return new PlatformCachePartialUpdateException((CachePartialUpdateCheckedException)e.getCause(),
                platformCtx, keepBinary);

        if (e instanceof CachePartialUpdateCheckedException)
            return new PlatformCachePartialUpdateException((CachePartialUpdateCheckedException)e, platformCtx, keepBinary);

        if (e.getCause() instanceof EntryProcessorException)
            return (Exception)e.getCause();

        TransactionDeadlockException deadlockException = X.cause(e, TransactionDeadlockException.class);

        if (deadlockException != null)
            return deadlockException;

        TransactionTimeoutException timeoutException = X.cause(e, TransactionTimeoutException.class);

        if (timeoutException != null)
            return timeoutException;

        return super.convertException(e);
    }

    /**
     * Writes the result of InvokeAll cache method.
     *
     * @param writer Writer.
     * @param results Results.
     */
    private static void writeInvokeAllResult(BinaryRawWriterEx writer, Map<Object, EntryProcessorResult> results) {
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

                PlatformUtils.writeError(ex, writer);
            }
        }
    }

    /**
     * Writes an error to the writer either as a native exception, or as a couple of strings.
     * @param writer Writer.
     * @param ex Exception.
     */
    private static void writeError(BinaryRawWriterEx writer, Exception ex) {
        if (ex.getCause() instanceof PlatformNativeException)
            writer.writeObjectDetached(((PlatformNativeException)ex.getCause()).cause());
        else {
            writer.writeObjectDetached(ex.getClass().getName());
            writer.writeObjectDetached(ex.getMessage());
            writer.writeObjectDetached(X.getFullStackTrace(ex));
        }
    }

    /** <inheritDoc /> */
    @Override public IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        return ((IgniteFutureImpl) cacheAsync.future()).internalFuture();
    }

    /** <inheritDoc /> */
    @Nullable @Override public PlatformFutureUtils.Writer futureWriter(int opId) {
        if (opId == OP_GET_ALL)
            return WRITER_GET_ALL;

        if (opId == OP_INVOKE)
            return WRITER_INVOKE;

        if (opId == OP_INVOKE_ALL)
            return WRITER_INVOKE_ALL;

        return null;
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
    private PlatformQueryCursor runQuery(Query qry) throws IgniteCheckedException {

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
    private PlatformFieldsQueryCursor runFieldsQuery(Query qry)
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
    private Query readInitialQuery(BinaryRawReaderEx reader) throws IgniteCheckedException {
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
    private Query readSqlQuery(BinaryRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        String sql = reader.readString();
        String typ = reader.readString();
        final int pageSize = reader.readInt();

        Object[] args = readQueryArgs(reader);

        boolean distrJoins = reader.readBoolean();

        return new SqlQuery(typ, sql).setPageSize(pageSize).setArgs(args).setLocal(loc).setDistributedJoins(distrJoins);
    }

    /**
     * Reads fields query.
     */
    private Query readFieldsQuery(BinaryRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        String sql = reader.readString();
        final int pageSize = reader.readInt();

        Object[] args = readQueryArgs(reader);

        boolean distrJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();

        return new SqlFieldsQuery(sql).setPageSize(pageSize).setArgs(args).setLocal(loc)
            .setDistributedJoins(distrJoins).setEnforceJoinOrder(enforceJoinOrder);
    }

    /**
     * Reads text query.
     */
    private Query readTextQuery(BinaryRawReader reader) {
        boolean loc = reader.readBoolean();
        String txt = reader.readString();
        String typ = reader.readString();
        final int pageSize = reader.readInt();

        return new TextQuery(typ, txt).setPageSize(pageSize).setLocal(loc);
    }

    /**
     * Reads scan query.
     */
    private Query readScanQuery(BinaryRawReaderEx reader) {
        boolean loc = reader.readBoolean();
        final int pageSize = reader.readInt();

        boolean hasPart = reader.readBoolean();

        Integer part = hasPart ? reader.readInt() : null;

        ScanQuery qry = new ScanQuery().setPageSize(pageSize);

        qry.setPartition(part);

        Object pred = reader.readObjectDetached();

        if (pred != null)
            qry.setFilter(platformCtx.createCacheEntryFilter(pred, 0));

        qry.setLocal(loc);

        return qry;
    }

    /**
     * Clones this instance.
     *
     * @param cache Cache.
     * @param keepBinary Keep binary flag.
     * @return Cloned instance.
     */
    private PlatformCache copy(IgniteCache cache, boolean keepBinary) {
        return new PlatformCache(platformCtx, cache, keepBinary, exts);
    }

    /**
     * Get extension by ID.
     *
     * @param id ID.
     * @return Extension.
     */
    private PlatformCacheExtension extension(int id) {
        if (exts != null && id < exts.length) {
            PlatformCacheExtension ext = exts[id];

            if (ext != null)
                return ext;
        }

        throw new IgniteException("Platform cache extension is not registered [id=" + id + ']');
    }

    /**
     * Writes cache metrics.
     *
     * @param writer Writer.
     * @param metrics Metrics.
     */
    public static void writeCacheMetrics(BinaryRawWriter writer, CacheMetrics metrics) {
        assert writer != null;
        assert metrics != null;

        writer.writeLong(metrics.getCacheHits());
        writer.writeFloat(metrics.getCacheHitPercentage());
        writer.writeLong(metrics.getCacheMisses());
        writer.writeFloat(metrics.getCacheMissPercentage());
        writer.writeLong(metrics.getCacheGets());
        writer.writeLong(metrics.getCachePuts());
        writer.writeLong(metrics.getCacheRemovals());
        writer.writeLong(metrics.getCacheEvictions());
        writer.writeFloat(metrics.getAverageGetTime());
        writer.writeFloat(metrics.getAveragePutTime());
        writer.writeFloat(metrics.getAverageRemoveTime());
        writer.writeFloat(metrics.getAverageTxCommitTime());
        writer.writeFloat(metrics.getAverageTxRollbackTime());
        writer.writeLong(metrics.getCacheTxCommits());
        writer.writeLong(metrics.getCacheTxRollbacks());
        writer.writeString(metrics.name());
        writer.writeLong(metrics.getOverflowSize());
        writer.writeLong(metrics.getOffHeapGets());
        writer.writeLong(metrics.getOffHeapPuts());
        writer.writeLong(metrics.getOffHeapRemovals());
        writer.writeLong(metrics.getOffHeapEvictions());
        writer.writeLong(metrics.getOffHeapHits());
        writer.writeFloat(metrics.getOffHeapHitPercentage());
        writer.writeLong(metrics.getOffHeapMisses());
        writer.writeFloat(metrics.getOffHeapMissPercentage());
        writer.writeLong(metrics.getOffHeapEntriesCount());
        writer.writeLong(metrics.getOffHeapPrimaryEntriesCount());
        writer.writeLong(metrics.getOffHeapBackupEntriesCount());
        writer.writeLong(metrics.getOffHeapAllocatedSize());
        writer.writeLong(metrics.getOffHeapMaxSize());
        writer.writeLong(metrics.getSwapGets());
        writer.writeLong(metrics.getSwapPuts());
        writer.writeLong(metrics.getSwapRemovals());
        writer.writeLong(metrics.getSwapHits());
        writer.writeLong(metrics.getSwapMisses());
        writer.writeLong(metrics.getSwapEntriesCount());
        writer.writeLong(metrics.getSwapSize());
        writer.writeFloat(metrics.getSwapHitPercentage());
        writer.writeFloat(metrics.getSwapMissPercentage());
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
    }

    /**
     * Writes error with EntryProcessorException cause.
     */
    private static class GetAllWriter implements PlatformFutureUtils.Writer {
        /** <inheritDoc /> */
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
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
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
            if (err == null) {
                writer.writeBoolean(false);  // No error.

                writer.writeObjectDetached(obj);
            }
            else {
                writer.writeBoolean(true);  // Error.

                PlatformUtils.writeError(err, writer);
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
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
            writeInvokeAllResult(writer, (Map)obj);
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return obj != null && err == null;
        }
    }

    /**
     * Listenable around CompletionListener.
     */
    private static class CompletionListenable implements PlatformListenable, CompletionListener {
        /** */
        private IgniteBiInClosure<Object, Throwable> lsnr;

        /** {@inheritDoc} */
        @Override public void onCompletion() {
            assert lsnr != null;

            lsnr.apply(null, null);
        }

        /** {@inheritDoc} */
        @Override public void onException(Exception e) {
            lsnr.apply(null, e);
        }

        /** {@inheritDoc} */
        @Override public void listen(IgniteBiInClosure<Object, Throwable> lsnr) {
            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return false;
        }
    }
}
