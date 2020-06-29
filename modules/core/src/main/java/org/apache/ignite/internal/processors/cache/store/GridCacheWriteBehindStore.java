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

package org.apache.ignite.internal.processors.cache.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static javax.cache.Cache.Entry;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.includeSensitive;

/**
 * Internal wrapper for a {@link CacheStore} that enables write-behind logic.
 * <p/>
 * The general purpose of this approach is to reduce cache store load under high
 * store update rate. The idea is to cache all write and remove operations in a pending
 * map and delegate these changes to the underlying store either after timeout or
 * if size of a pending map exceeded some pre-configured value. Another performance gain
 * is achieved due to combining a group of similar operations to a single batch update.
 * <p/>
 * The essential flush size for the write-behind cache should be at least the estimated
 * count of simultaneously written keys. In case of significantly smaller value there would
 * be triggered a lot of flush events that will result in a high cache store load.
 * <p/>
 * Since write operations to the cache store are deferred, transaction support is lost; no
 * transaction objects are passed to the underlying store.
 * <p/>
 * {@link GridCacheWriteBehindStore} doesn't support concurrent modifications of the same key.
 */
public class GridCacheWriteBehindStore<K, V> implements CacheStore<K, V>, LifecycleAware {
    /** Default write cache initial capacity. */
    public static final int DFLT_INITIAL_CAPACITY = 1024;

    /** Overflow ratio for critical cache size calculation. */
    public static final float CACHE_OVERFLOW_RATIO = 1.5f;

    /** Default concurrency level of write cache. */
    public static final int DFLT_CONCUR_LVL = 64;

    /** Write cache initial capacity. */
    private int initCap = DFLT_INITIAL_CAPACITY;

    /** Concurrency level for write cache access. */
    private int concurLvl = DFLT_CONCUR_LVL;

    /** When cache size exceeds this value eldest entry will be stored to the underlying store. */
    private int cacheMaxSize = CacheConfiguration.DFLT_WRITE_BEHIND_FLUSH_SIZE;

    /** Critical cache size. If cache size exceeds this value, data flush performed synchronously. */
    private int cacheCriticalSize;

    /** Count of worker threads performing underlying store updates. */
    private int flushThreadCnt = CacheConfiguration.DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT;

    /** Is flush threads count power of two flag. */
    private boolean flushThreadCntIsPowerOfTwo;

    /** Cache flush frequency. All pending operations will be performed in not less then this value ms. */
    private long cacheFlushFreq = CacheConfiguration.DFLT_WRITE_BEHIND_FLUSH_FREQUENCY;

    /** Maximum batch size for put and remove operations */
    private int batchSize = CacheConfiguration.DFLT_WRITE_BEHIND_BATCH_SIZE;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Cache name. */
    private final String cacheName;

    /** Underlying store. */
    private final CacheStore<K, V> store;

    /** Write cache. */
    private ConcurrentLinkedHashMap<K, StatefulValue<K, V>> writeCache;

    /** Flusher threads. */
    private Flusher[] flushThreads;

    /** Write coalescing. */
    private boolean writeCoalescing = CacheConfiguration.DFLT_WRITE_BEHIND_COALESCING;

    /** Atomic flag indicating store shutdown. */
    private AtomicBoolean stopping = new AtomicBoolean(true);

    /** Variable for counting total cache overflows. */
    private AtomicInteger cacheTotalOverflowCntr = new AtomicInteger();

    /** Variable contains current number of overflow events. */
    private AtomicInteger cacheOverflowCntr = new AtomicInteger();

    /** Variable for counting key-value pairs that are in {@link ValueStatus#RETRY} state. */
    private AtomicInteger retryEntriesCnt = new AtomicInteger();

    /** Log. */
    private final IgniteLogger log;

    /** Store manager. */
    private final CacheStoreManager storeMgr;

    /** Flush lock. */
    private final Lock flushLock = new ReentrantLock();

    /** Condition to determine records available for flush. */
    private Condition canFlush = flushLock.newCondition();

    /**
     * Creates a write-behind cache store for the given store.
     *
     * @param storeMgr Store manager.
     * @param igniteInstanceName Ignite instance name.
     * @param cacheName Cache name.
     * @param log Grid logger.
     * @param store {@code GridCacheStore} that need to be wrapped.
     */
    public GridCacheWriteBehindStore(
        CacheStoreManager storeMgr,
        String igniteInstanceName,
        String cacheName,
        IgniteLogger log,
        CacheStore<K, V> store) {
        this.storeMgr = storeMgr;
        this.igniteInstanceName = igniteInstanceName;
        this.cacheName = cacheName;
        this.log = log;
        this.store = store;
    }

    /**
     * Sets initial capacity for the write cache.
     *
     * @param initCap Initial capacity.
     */
    public void setInitialCapacity(int initCap) {
        this.initCap = initCap;
    }

    /**
     * Sets concurrency level for the write cache. Concurrency level is expected count of concurrent threads
     * attempting to update cache.
     *
     * @param concurLvl Concurrency level.
     */
    public void setConcurrencyLevel(int concurLvl) {
        this.concurLvl = concurLvl;
    }

    /**
     * Sets the maximum size of the write cache. When the count of unique keys in write cache exceeds this value,
     * the eldest entry in the cache is immediately scheduled for write to the underlying store.
     *
     * @param cacheMaxSize Max cache size.
     */
    public void setFlushSize(int cacheMaxSize) {
        this.cacheMaxSize = cacheMaxSize;
    }

    /**
     * Gets the maximum size of the write-behind buffer. When the count of unique keys
     * in write buffer exceeds this value, the buffer is scheduled for write to the underlying store.
     * <p/>
     * If this value is {@code 0}, then flush is performed only on time-elapsing basis. However,
     * when this value is {@code 0}, the cache critical size is set to
     * {@link CacheConfiguration#DFLT_WRITE_BEHIND_CRITICAL_SIZE}.
     *
     * @return Buffer size that triggers flush procedure.
     */
    public int getWriteBehindFlushSize() {
        return cacheMaxSize;
    }

    /**
     * Sets the number of threads that will perform store update operations.
     *
     * @param flushThreadCnt Count of worker threads.
     */
    public void setFlushThreadCount(int flushThreadCnt) {
        this.flushThreadCnt = flushThreadCnt;
        this.flushThreadCntIsPowerOfTwo = (flushThreadCnt & (flushThreadCnt - 1)) == 0;
    }

    /**
     * Gets the number of flush threads that will perform store update operations.
     *
     * @return Count of worker threads.
     */
    public int getWriteBehindFlushThreadCount() {
        return flushThreadCnt;
    }

    /**
     * Sets the write coalescing flag.
     *
     * @param writeCoalescing Write coalescing flag.
     */
    public void setWriteCoalescing(boolean writeCoalescing) {
        this.writeCoalescing = writeCoalescing;
    }

    /**
     * Gets the write coalescing flag.
     *
     * @return Write coalescing flag.
     */
    public boolean getWriteCoalescing() {
        return writeCoalescing;
    }

    /**
     * Sets the cache flush frequency. All pending operations on the underlying store will be performed
     * within time interval not less then this value.
     *
     * @param cacheFlushFreq Time interval value in milliseconds.
     */
    public void setFlushFrequency(long cacheFlushFreq) {
        this.cacheFlushFreq = cacheFlushFreq;
    }

    /**
     * Gets the cache flush frequency. All pending operations on the underlying store will be performed
     * within time interval not less then this value.
     * <p/>
     * If this value is {@code 0}, then flush is performed only when buffer size exceeds flush size.
     *
     * @return Flush frequency in milliseconds.
     */
    public long getWriteBehindFlushFrequency() {
        return cacheFlushFreq;
    }

    /**
     * Sets the maximum count of similar operations that can be grouped to a single batch.
     *
     * @param batchSize Maximum count of batch.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Gets the maximum count of similar (put or remove) operations that can be grouped to a single batch.
     *
     * @return Maximum size of batch.
     */
    public int getWriteBehindStoreBatchSize() {
        return batchSize;
    }

    /**
     * Gets count of entries that were processed by the write-behind store and have not been
     * flushed to the underlying store yet.
     *
     * @return Total count of entries in cache store internal buffer.
     */
    public int getWriteBehindBufferSize() {
        if (writeCoalescing)
            return writeCache.sizex();
        else {
            int size = 0;

            for (Flusher f : flushThreads)
                size += f.size();

            return size;
        }
    }

    /**
     * @return Underlying store.
     */
    public CacheStore<K, V> store() {
        return store;
    }

    /**
     * Performs all the initialization logic for write-behind cache store.
     * This class must not be used until this method returns.
     */
    @Override public void start() {
        assert cacheFlushFreq != 0 || cacheMaxSize != 0;

        if (stopping.compareAndSet(true, false)) {
            if (log.isDebugEnabled())
                log.debug("Starting write-behind store for cache '" + cacheName + '\'');

            cacheCriticalSize = (int)(cacheMaxSize * CACHE_OVERFLOW_RATIO);

            if (cacheCriticalSize == 0)
                cacheCriticalSize = CacheConfiguration.DFLT_WRITE_BEHIND_CRITICAL_SIZE;

            flushThreads = new GridCacheWriteBehindStore.Flusher[flushThreadCnt];

            if (writeCoalescing)
                writeCache = new ConcurrentLinkedHashMap<>(initCap, 0.75f, concurLvl);

            for (int i = 0; i < flushThreads.length; i++) {
                flushThreads[i] = new Flusher(igniteInstanceName, "flusher-" + i, log);

                flushThreads[i].start();
            }
        }
    }

    /**
     * Gets count of write buffer overflow events since initialization. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    public int getWriteBehindTotalCriticalOverflowCount() {
        return cacheTotalOverflowCntr.get();
    }

    /**
     * Gets count of write buffer overflow events in progress at the moment. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    public int getWriteBehindCriticalOverflowCount() {
        return cacheOverflowCntr.get();
    }

    /**
     * Gets count of cache entries that are in a store-retry state. An entry is assigned a store-retry state
     * when underlying store failed due some reason and cache has enough space to retain this entry till
     * the next try.
     *
     * @return Count of entries in store-retry state.
     */
    public int getWriteBehindErrorRetryCount() {
        return retryEntriesCnt.get();
    }

    /**
     * Performs shutdown logic for store. No put, get and remove requests will be processed after
     * this method is called.
     */
    @Override public void stop() {
        if (stopping.compareAndSet(false, true)) {
            if (log.isDebugEnabled())
                log.debug("Stopping write-behind store for cache '" + cacheName + '\'');

            for (Flusher f : flushThreads) {
                if (!f.isEmpty())
                    f.wakeUp();
            }

            boolean graceful = true;

            for (GridWorker worker : flushThreads)
                graceful &= U.join(worker, log);

            if (!graceful)
                log.warning("Write behind store shutdown was aborted.");
        }
    }

    /**
     * Forces all entries collected to be flushed to the underlying store.
     * @throws IgniteCheckedException If failed.
     */
    public void forceFlush() throws IgniteCheckedException {
        for (Flusher f : flushThreads) {
            if (!f.isEmpty())
                f.wakeUp();
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) {
        store.loadCache(clo, args);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        if (log.isDebugEnabled())
            log.debug(S.toString("Store load all",
                "keys", keys, true));

        Map<K, V> loaded = new HashMap<>();

        Collection<K> remaining = null;

        for (K key : keys) {
            StatefulValue<K, V> val;

            if (writeCoalescing)
                val = writeCache.get(key);
            else
                val = flusher(key).flusherWriteMap.get(key);

            if (val != null) {
                val.readLock().lock();

                try {
                    StoreOperation op;

                    V value;

                    if (writeCoalescing && val.nextOperation() != null) {
                        op = val.nextOperation();

                        value = (op == StoreOperation.PUT) ? val.nextEntry().getValue() : null;
                    } else {
                        op = val.operation();

                        value = (op == StoreOperation.PUT) ? val.entry().getValue() : null;
                    }

                    if (op == StoreOperation.PUT)
                        loaded.put(key, value);
                    else
                        assert op == StoreOperation.RMV : op;
                }
                finally {
                    val.readLock().unlock();
                }
            }
            else {
                if (remaining == null)
                    remaining = new ArrayList<>();

                remaining.add(key);
            }
        }

        // For items that were not found in queue.
        if (remaining != null && !remaining.isEmpty()) {
            Map<K, V> loaded0 = store.loadAll(remaining);

            if (loaded0 != null)
                loaded.putAll(loaded0);
        }

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public V load(K key) {
        if (log.isDebugEnabled())
            log.debug(S.toString("Store load",
                "key", key, true));

        StatefulValue<K, V> val;

        if (writeCoalescing)
            val = writeCache.get(key);
        else
            val = flusher(key).flusherWriteMap.get(key);

        if (val != null) {
            val.readLock().lock();

            try {
                StoreOperation op;

                V value;

                if (writeCoalescing && val.nextOperation() != null) {
                    op = val.nextOperation();

                    value = (op == StoreOperation.PUT) ? val.nextEntry().getValue() : null;
                } else {
                    op = val.operation();

                    value = (op == StoreOperation.PUT) ? val.entry().getValue() : null;
                }

                switch (op) {
                    case PUT:
                        return value;

                    case RMV:
                        return null;

                    default:
                        assert false : "Unexpected operation: " + val.status();
                }
            }
            finally {
                val.readLock().unlock();
            }
        }

        return store.load(key);
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Entry<? extends K, ? extends V>> entries) {
        for (Entry<? extends K, ? extends V> e : entries)
            write(e);
    }

    /** {@inheritDoc} */
    @Override public void write(Entry<? extends K, ? extends V> entry) {
        try {
            if (log.isDebugEnabled())
                log.debug(S.toString("Store put",
                    "key", entry.getKey(), true,
                    "val", entry.getValue(), true));

            updateCache(entry.getKey(), entry, StoreOperation.PUT);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        for (Object key : keys)
            delete(key);
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        try {
            if (log.isDebugEnabled())
                log.debug(S.toString("Store remove",
                    "key", key, true));

            updateCache((K)key, null, StoreOperation.RMV);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new CacheWriterException(U.convertExceptionNoWrap(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheWriteBehindStore.class, this);
    }

    /**
     * Performs flush-consistent cache update for the given key.
     *
     * @param key Key for which update is performed.
     * @param val New value, may be null for remove operation.
     * @param operation Updated value status.
     * @throws IgniteInterruptedCheckedException If interrupted while waiting for value to be flushed.
     */
    private void updateCache(K key,
        @Nullable Entry<? extends K, ? extends V> val,
        StoreOperation operation)
        throws IgniteInterruptedCheckedException {
        StatefulValue<K, V> newVal = new StatefulValue<>(val, operation);

        if (writeCoalescing)
            putToWriteCache(key, newVal);
        else
            flusher(key).putToFlusherWriteCache(key, newVal);
    }

    /**
     * Performs flush-consistent writeCache update for the given key.
     *
     * @param key Key for which update is performed.
     * @param newVal stateful value to put
     * @throws IgniteInterruptedCheckedException If interrupted while waiting for value to be flushed.
     */
    private void putToWriteCache(
        K key,
        StatefulValue<K, V> newVal)
        throws IgniteInterruptedCheckedException {
        StatefulValue<K, V> prev;

        assert writeCoalescing : "Unexpected write coalescing.";

        while ((prev = writeCache.putIfAbsent(key, newVal)) != null) {
            prev.writeLock().lock();

            try {
                if (prev.status() == ValueStatus.PENDING || prev.status() == ValueStatus.PENDING_AND_UPDATED) {
                    // Flush process in progress, save next value and update the status.

                    prev.setNext(newVal.val, newVal.storeOperation);

                    prev.status(ValueStatus.PENDING_AND_UPDATED);

                    break;
                }
                else if (prev.status() == ValueStatus.FLUSHED)
                    // This entry was deleted from map before we acquired the lock.
                    continue;
                else if (prev.status() == ValueStatus.RETRY)
                    // New value has come, old value is no longer in RETRY state,
                    retryEntriesCnt.decrementAndGet();

                assert prev.status() == ValueStatus.NEW || prev.status() == ValueStatus.RETRY;

                prev.update(newVal.val, newVal.operation(), ValueStatus.NEW);

                break;
            }
            finally {
                prev.writeLock().unlock();
            }
        }

        // Now check the map size
        int cacheSize = getWriteBehindBufferSize();

        if (cacheSize > cacheCriticalSize)
            // Perform single store update in the same thread.
            flushSingleValue();
        else if (cacheMaxSize > 0 && cacheSize > cacheMaxSize)
            wakeUp();
    }

    /**
     * Return flusher by by key.
     *
     * @param key Key for search.
     * @return flusher.
     */
    private Flusher flusher(K key) {
        int h, idx;

        if (flushThreadCntIsPowerOfTwo)
            idx = ((h = key.hashCode()) ^ (h >>> 16)) & (flushThreadCnt - 1);
        else
            idx = ((h = key.hashCode()) ^ (h >>> 16)) % flushThreadCnt;

        return flushThreads[idx];
    }

    /**
     * Flushes one upcoming value to the underlying store. Called from
     * {@link #updateCache(Object, Entry, StoreOperation)} method in case when current map size exceeds
     * critical size.
     */
    private void flushSingleValue() {
        cacheOverflowCntr.incrementAndGet();

        try {
            Map<K, StatefulValue<K, V>> batch;

            for (Map.Entry<K, StatefulValue<K, V>> e : writeCache.entrySet()) {
                StatefulValue<K, V> val = e.getValue();

                val.writeLock().lock();

                try {
                    ValueStatus status = val.status();

                    if (acquired(status))
                        // Another thread is helping us, continue to the next entry.
                        continue;

                    if (val.status() == ValueStatus.RETRY)
                        retryEntriesCnt.decrementAndGet();

                    assert retryEntriesCnt.get() >= 0;

                    val.status(ValueStatus.PENDING);

                    batch = Collections.singletonMap(e.getKey(), val);
                }
                finally {
                    val.writeLock().unlock();
                }

                if (!batch.isEmpty()) {
                    applyBatch(batch, false, null);

                    cacheTotalOverflowCntr.incrementAndGet();

                    return;
                }
            }
        }
        finally {
            cacheOverflowCntr.decrementAndGet();
        }
    }

    /**
     * Performs batch operation on underlying store.
     *
     * @param valMap Batch map.
     * @param initSes {@code True} if need to initialize session.
     * @param flusher Flusher, assotiated with all keys in batch (have sense in write coalescing = false mode)
     * @return {@code True} if batch was successfully applied, {@code False} otherwise.
     */
    private boolean applyBatch(Map<K, StatefulValue<K, V>> valMap, boolean initSes, Flusher flusher) {
        assert valMap.size() <= batchSize;
        assert !valMap.isEmpty();

        StoreOperation operation = null;

        // Construct a map for underlying store
        Map<K, Entry<? extends K, ? extends V>> batch = U.newLinkedHashMap(valMap.size());

        for (Map.Entry<K, StatefulValue<K, V>> e : valMap.entrySet()) {
            StatefulValue<K, V> val = e.getValue();

            val.readLock().lock();

            try {
                if (operation == null)
                    operation = val.operation();

                assert operation == val.operation();

                assert val.status() == ValueStatus.PENDING || val.status() == ValueStatus.PENDING_AND_UPDATED;

                batch.put(e.getKey(), val.entry());
            } finally {
                val.readLock().unlock();
            }
        }

        boolean result = updateStore(operation, batch, initSes, flusher);

        if (result) {
            for (Map.Entry<K, StatefulValue<K, V>> e : valMap.entrySet()) {
                StatefulValue<K, V> val = e.getValue();

                val.writeLock().lock();

                try {
                    if (writeCoalescing) {
                        if (val.status() == ValueStatus.PENDING_AND_UPDATED) {
                            val.update(val.nextEntry(), val.nextOperation(), ValueStatus.NEW);

                            val.setNext(null, null);
                        }
                        else {
                            val.status(ValueStatus.FLUSHED);

                            StatefulValue<K, V> prev = writeCache.remove(e.getKey());

                            // Additional check to ensure consistency.
                            assert prev == val : "Map value for key " + e.getKey() + " was updated during flush";
                        }

                        val.signalFlushed();
                    }
                    else {
                        val.status(ValueStatus.FLUSHED);

                        Flusher f = flusher(e.getKey());

                        // Can remove using equal because if map contains another similar value it has different state.
                        f.flusherWriteMap.remove(e.getKey(), e.getValue());

                        val.signalFlushed();
                    }
                }
                finally {
                    val.writeLock().unlock();
                }
            }
        }
        else {
            // Exception occurred, we must set RETRY status
            for (StatefulValue<K, V> val : valMap.values()) {
                val.writeLock().lock();

                try {
                    if (val.status() == ValueStatus.PENDING_AND_UPDATED) {
                        val.update(val.nextEntry(), val.nextOperation(), ValueStatus.NEW);

                        val.setNext(null, null);
                    }
                    else {
                        val.status(ValueStatus.RETRY);

                        retryEntriesCnt.incrementAndGet();
                    }

                    val.signalFlushed();
                }
                finally {
                    val.writeLock().unlock();
                }
            }
        }

        return result;
    }

    /**
     * Tries to update store with the given values and returns {@code true} in case of success.
     *
     * <p/> If any exception in underlying store is occurred, this method checks the map size.
     * If map size exceeds some critical value, then it returns {@code true} and this value will
     * be lost. If map size does not exceed critical value, it will return false and value will
     * be retained in write cache.
     *
     * @param operation Status indicating operation that should be performed.
     * @param vals Key-Value map.
     * @param initSes {@code True} if need to initialize session.
     * @param flusher Flusher, assotiated with vals keys (in writeCoalescing=false mode)
     * @return {@code true} if value may be deleted from the write cache,
     *         {@code false} otherwise
     */
    private boolean updateStore(
        StoreOperation operation,
        Map<K, Entry<? extends K, ? extends V>> vals,
        boolean initSes,
        Flusher flusher
    ) {
        try {
            if (storeMgr != null) {
                if (initSes)
                    storeMgr.writeBehindSessionInit();
                else
                    // Back-pressure mechanism is running.
                    // Cache store session must be initialized by storeMgr.
                    storeMgr.writeBehindCacheStoreSessionListenerStart();
            }

            boolean threwEx = true;

            try {
                switch (operation) {
                    case PUT:
                        store.writeAll(vals.values());

                        break;

                    case RMV:
                        store.deleteAll(vals.keySet());

                        break;

                    default:
                        assert false : "Unexpected operation: " + operation;
                }

                threwEx = false;

                return true;
            }
            finally {
                if (initSes && storeMgr != null)
                    storeMgr.writeBehindSessionEnd(threwEx);
            }
        }
        catch (Exception e) {
            LT.warn(log, e, "Unable to update underlying store: " + store, false, false);

            boolean overflow;

            if (writeCoalescing)
                overflow = writeCache.sizex() > cacheCriticalSize || stopping.get();
            else
                overflow = flusher.isOverflowed() || stopping.get();

            if (overflow) {
                for (Map.Entry<K, Entry<? extends K, ? extends V>> entry : vals.entrySet()) {
                    Object val = entry.getValue() != null ? entry.getValue().getValue() : null;

                    log.error("Failed to update store (value will be lost as current buffer size is greater " +
                        "than 'cacheCriticalSize' or node has been stopped before store was repaired) [" +
                        (includeSensitive() ? "key=" + entry.getKey() + ", val=" + val + ", " : "") +
                        "op=" + operation + "]");
                }

                return true;
            }

            return false;
        }
    }

    /**
     * Wakes up flushing threads if map size exceeded maximum value or in case of shutdown.
     */
    private void wakeUp() {
        flushLock.lock();

        try {
            canFlush.signalAll();
        }
        finally {
            flushLock.unlock();
        }
    }

    /**
     * Thread that performs time/size-based flushing of written values to the underlying storage.
     */
    private class Flusher extends GridWorker {
        /** Queue to flush. */
        private final FastSizeDeque<IgniteBiTuple<K, StatefulValue<K,V>>> queue;

        /** Flusher write map. */
        private final ConcurrentHashMap<K, StatefulValue<K,V>> flusherWriteMap;

        /** Critical size of flusher local queue. */
        private final int flusherCacheCriticalSize;

        /** Flusher parked flag. */
        private volatile boolean parked;

        /** Flusher thread. */
        protected Thread thread;

        /** Cache flushing frequence in nanos. */
        protected long cacheFlushFreqNanos = cacheFlushFreq * 1000 * 1000;

        /** Writer lock. */
        private final Lock flusherWriterLock = new ReentrantLock();

        /** Confition to determine available space for flush. */
        private Condition flusherWriterCanWrite = flusherWriterLock.newCondition();

        /** {@inheritDoc */
        protected Flusher(String igniteInstanceName,
            String name,
            IgniteLogger log) {
            super(igniteInstanceName, name, log);

            flusherCacheCriticalSize = cacheCriticalSize / flushThreadCnt;

            assert flusherCacheCriticalSize > batchSize;

            if (writeCoalescing) {
                queue = null;
                flusherWriteMap = null;
            }
            else {
                queue = new FastSizeDeque<>(new ConcurrentLinkedDeque<>());
                flusherWriteMap = new ConcurrentHashMap<>(initCap, 0.75f, concurLvl);
            }
        }

        /** Start flusher thread */
        protected void start() {
            thread = new IgniteThread(this);
            thread.start();
        }

        /**
         * Performs flush-consistent flusher writeCache update for the given key.
         *
         * @param key Key for which update is performed.
         * @param newVal stateful value to put
         * @throws IgniteInterruptedCheckedException If interrupted while waiting for value to be flushed.
         */
        private void putToFlusherWriteCache(
            K key,
            StatefulValue<K, V> newVal
        ) throws IgniteInterruptedCheckedException {
            assert !writeCoalescing : "Unexpected write coalescing.";

            if (queue.sizex() > flusherCacheCriticalSize) {
                while (queue.sizex() > flusherCacheCriticalSize) {
                    wakeUp();

                    flusherWriterLock.lock();

                    try {
                        // Wait for free space in flusher queue
                        while (queue.sizex() >= flusherCacheCriticalSize && !stopping.get()) {
                            if (cacheFlushFreq > 0)
                                flusherWriterCanWrite.await(cacheFlushFreq, TimeUnit.MILLISECONDS);
                            else
                                flusherWriterCanWrite.await();
                        }

                        cacheTotalOverflowCntr.incrementAndGet();
                    }
                    catch (InterruptedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Caught interrupted exception: " + e);

                        Thread.currentThread().interrupt();
                    }
                    finally {
                        flusherWriterLock.unlock();
                    }
                }

                cacheTotalOverflowCntr.incrementAndGet();
            }

            queue.add(F.t(key, newVal));

            flusherWriteMap.put(key, newVal);
        }

        /**
         * Get overflowed flag.
         *
         * @return {@code True} if write behind flusher is overflowed,
         *         {@code False} otherwise.
         */
        public boolean isOverflowed() {
            if (writeCoalescing)
                return writeCache.sizex() > cacheCriticalSize;
            else
                return queue.sizex() > flusherCacheCriticalSize;
        }

        /**
         * Get write behind flusher size.
         *
         * @return Flusher write behind size.
         */
        public int size() {
            return writeCoalescing ? writeCache.sizex() : queue.sizex();
        }

        /**
         * Test if write behind flusher is empty
         *
         * @return {@code True} if write behind flusher is empty, {@code False} otherwise
         */
        public boolean isEmpty() {
            return writeCoalescing ? writeCache.isEmpty() : queue.isEmpty();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (writeCoalescing) {
                while (!stopping.get() || writeCache.sizex() > 0) {
                    awaitOperationsAvailableCoalescing();

                    flushCacheCoalescing();
                }
            }
            else {
                while (!stopping.get() || queue.sizex() > 0) {
                    awaitOperationsAvailableNonCoalescing();

                    flushCacheNonCoalescing();
                }
            }
        }

        /**
         * This method awaits until enough elements in flusher queue are available or given timeout is over.
         *
         * @throws InterruptedException If awaiting was interrupted.
         */
        private void awaitOperationsAvailableCoalescing() throws InterruptedException {
            flushLock.lock();

            try {
                do {
                    if (writeCache.sizex() <= cacheMaxSize || cacheMaxSize == 0) {
                        if (cacheFlushFreq > 0)
                            canFlush.await(cacheFlushFreq, TimeUnit.MILLISECONDS);
                        else
                            canFlush.await();
                    }
                }
                while (writeCache.sizex() == 0 && !stopping.get());
            }
            finally {
                flushLock.unlock();
            }
        }

        /**
         * This method awaits until enough elements in flusher queue are available or given timeout is over.
         *
         * @throws InterruptedException If awaiting was interrupted.
         */
        private void awaitOperationsAvailableNonCoalescing() throws InterruptedException {
            if (queue.sizex() >= batchSize)
                return;

            parked = true;

            try {
                for (;;) {
                    if (queue.sizex() >= batchSize)
                        return;

                    if (cacheFlushFreq > 0)
                        LockSupport.parkNanos(cacheFlushFreqNanos);
                    else
                        LockSupport.park();

                    if (queue.sizex() > 0)
                        return;

                    if (Thread.interrupted())
                        throw new InterruptedException();

                    if (stopping.get())
                        return;
                }
            }
            finally {
                parked = false;
            }
        }

        /**
         * Wake up flusher thread.
         */
        public void wakeUp() {
            if (parked)
                LockSupport.unpark(thread);
        }

        /**
         * Removes values from the write cache and performs corresponding operation
         * on the underlying store.
         */
        private void flushCacheCoalescing() {
            StoreOperation prevOperation = null;

            Map<K, StatefulValue<K, V>> pending = U.newLinkedHashMap(batchSize);
            Iterator<Map.Entry<K, StatefulValue<K, V>>> it = writeCache.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<K, StatefulValue<K, V>> e = it.next();
                StatefulValue<K, V> val = e.getValue();

                if (!val.writeLock().tryLock()) // TODO: stripe write maps to avoid lock contention.
                    continue;

                try {
                    BatchingResult addRes = tryAddStatefulValue(pending, prevOperation, e.getKey(), val);

                    switch (addRes) {
                        case NEW_BATCH:
                            // No need to test first value in batch
                            val.status(ValueStatus.PENDING);

                            val.writeLock().unlock();

                            applyBatch(pending, true, null);

                            pending = U.newLinkedHashMap(batchSize);

                            pending.put(e.getKey(), val);

                            prevOperation = val.operation();

                            break;

                        case ADDED:
                            prevOperation = val.operation();

                            break;

                        default:
                            assert addRes == BatchingResult.SKIPPED : "Unexpected result: " + addRes;
                    }
                }
                finally {
                    if (val.writeLock().isHeldByCurrentThread())
                        val.writeLock().unlock();
                }
            }

            // Process the remainder.
            if (!pending.isEmpty())
                applyBatch(pending, true, null);
        }

        /**
         * Removes values from the flusher write queue and performs corresponding operation
         * on the underlying store.
         */
        private void flushCacheNonCoalescing() {
            StoreOperation prevOperation;
            Map<K, StatefulValue<K, V>> pending;
            IgniteBiTuple<K, StatefulValue<K, V>> tuple;
            boolean applied;

            while (!queue.isEmpty()) {
                pending = U.newLinkedHashMap(batchSize);
                prevOperation = null;
                boolean needNewBatch = false;

                // Collect batch
                while (!needNewBatch && (tuple = queue.peek()) != null) {
                    BatchingResult addRes = tryAddStatefulValue(pending, prevOperation, tuple.getKey(),
                        tuple.getValue());

                    switch (addRes) {
                        case ADDED:
                            prevOperation = tuple.getValue().operation();
                            queue.poll();

                            break;

                        case SKIPPED:
                            assert false : "Unexpected result: " + addRes;

                            break;

                        case NEW_BATCH:
                            needNewBatch = true;
                            prevOperation = null;

                            break;

                        default:
                            assert false : "Unexpected result: " + addRes;
                    }
                }

                // Process collected batch
                applied = applyBatch(pending, true, this);

                if (applied) {
                    // Wake up awaiting writers
                    flusherWriterLock.lock();

                    try {
                        flusherWriterCanWrite.signalAll();
                    }
                    finally {
                        flusherWriterLock.unlock();
                    }
                }
                else {
                    // Return values to queue
                    ArrayList<Map.Entry<K, StatefulValue<K,V>>> pendingList = new ArrayList(pending.entrySet());

                    for (int i = pendingList.size() - 1; i >= 0; i--)
                        queue.addFirst(F.t(pendingList.get(i).getKey(), pendingList.get(i).getValue()));
                }
            }
        }

        /**
         * Trying to add key and statefull value pairs into pending map.
         *
         * @param pending Map to populate.
         * @param key Key to add.
         * @param val Stateful value to add.
         * @return {@code BatchingResult.ADDED} if pair was sucessfully added,
         *     {@code BatchingResult.SKIPPED} if pair cannot be processed by this thread,
         *     {@code BatchingResult.NEW_BATCH} if pair require new batch (pending map) to be added.
         */
        public BatchingResult tryAddStatefulValue(
            Map<K, StatefulValue<K, V>> pending,
            StoreOperation prevOperation,
            K key,
            StatefulValue<K, V> val
        ) {
            ValueStatus status = val.status();

            assert !(pending.isEmpty() && prevOperation != null) : "prev operation cannot be " + prevOperation
                + " if prev map is empty!";

            if (acquired(status))
                // Another thread is helping us, continue to the next entry.
                return BatchingResult.SKIPPED;

            if (!writeCoalescing && pending.containsKey(key))
                return BatchingResult.NEW_BATCH;

            if (status == ValueStatus.RETRY)
                retryEntriesCnt.decrementAndGet();

            assert retryEntriesCnt.get() >= 0;

            if (pending.size() == batchSize)
                return BatchingResult.NEW_BATCH;

            // We scan for the next operation and apply batch on operation change. Null means new batch.
            if (prevOperation != val.operation() && prevOperation != null)
                // Operation is changed, so we need to perform a batch.
                return BatchingResult.NEW_BATCH;
            else {
                val.status(ValueStatus.PENDING);

                pending.put(key, val);

                return BatchingResult.ADDED;
            }
        }
    }

    /**
     * For test purposes only.
     *
     * @return Write cache for the underlying store operations.
     */
    Map<K, StatefulValue<K, V>> writeCache() {
        return writeCache;
    }

    /**
     * For test purposes only.
     *
     * @return Flusher maps for the underlying store operations.
     */
    Map<K, StatefulValue<K,V>>[] flusherMaps() {
        Map<K, StatefulValue<K,V>>[] result = new Map[flushThreadCnt];

        for (int i = 0; i < flushThreadCnt; i++)
            result[i] = flushThreads[i].flusherWriteMap;

        return result;
    }

    /**
     * Enumeration that represents possible operations on the underlying store.
     */
    private enum StoreOperation {
        /** Put key-value pair to the underlying store. */
        PUT,

        /** Remove key from the underlying store. */
        RMV
    }

    /**
     * Enumeration that represents possible states of value in the map.
     */
    private enum ValueStatus {
        /** Value is scheduled for write or delete from the underlying cache but has not been captured by flusher. */
        NEW,

        /** Value is captured by flusher and store operation is performed at the moment. */
        PENDING,

        /**
         * Value is captured by flusher and store operation is performed at the moment.
         * New update for the key was stored and waiting for previous store operation.
         */
        PENDING_AND_UPDATED,

        /** Store operation has failed and it will be re-tried at the next flush. */
        RETRY,

        /** Store operation succeeded and this value will be removed by flusher. */
        FLUSHED,
    }

    /**
     * Enumeration that represents possible result of "add to batch" operation.
     */
    private enum BatchingResult {
        /** Added to batch */
        ADDED,

        /** Skipped. */
        SKIPPED,

        /** Need new batch. */
        NEW_BATCH
    }

    /**
     * Checks if given status indicates pending or complete flush operation.
     *
     * @param status Status to check.
     * @return {@code true} if status indicates any pending or complete store update operation.
     */
    private boolean acquired(ValueStatus status) {
        return status == ValueStatus.PENDING || status == ValueStatus.FLUSHED || status == ValueStatus.PENDING_AND_UPDATED;
    }

    /**
     * A state-value-operation trio.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private static class StatefulValue<K, V> extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Value. */
        @GridToStringInclude(sensitive = true)
        private Entry<? extends K, ? extends V> val;

        /** Next value that waiting for a previous store operation. */
        @GridToStringInclude(sensitive = true)
        private Entry<? extends K, ? extends V> nextVal;

        /** Store operation. */
        private StoreOperation storeOperation;

        /** Next store operation. */
        private StoreOperation nextStoreOperation;

        /** Value status. */
        private ValueStatus valStatus;

        /** Condition to wait for flush event */
        private Condition flushCond = writeLock().newCondition();

        /**
         * Creates a state-value pair with {@link ValueStatus#NEW} status.
         *
         * @param val Value.
         * @param storeOperation Store operation.
         */
        private StatefulValue(Entry<? extends K, ? extends V> val, StoreOperation storeOperation) {
            assert storeOperation == StoreOperation.PUT || storeOperation == StoreOperation.RMV;

            this.val = val;
            this.storeOperation = storeOperation;
            valStatus = ValueStatus.NEW;
        }

        /**
         * @return Next stored value.
         */
        private Entry<? extends K, ? extends V> nextEntry() {
            return nextVal;
        }

        /**
         * @return Store operation.
         */
        private StoreOperation nextOperation() {
            return nextStoreOperation;
        }

        /**
         * @return Stored value.
         */
        private Entry<? extends K, ? extends V> entry() {
            return val;
        }

        /**
         * @return Store operation.
         */
        private StoreOperation operation() {
            return storeOperation;
        }

        /**
         * @return Value status.
         */
        private ValueStatus status() {
            return valStatus;
        }

        /**
         * Updates value status.
         *
         * @param valStatus Value status.
         */
        private void status(ValueStatus valStatus) {
            this.valStatus = valStatus;
        }

        /**
         * Updates both value and value status.
         *
         * @param val Value.
         * @param storeOperation Store operation.
         * @param valStatus Value status.
         */
        private void update(@Nullable Entry<? extends K, ? extends V> val,
            StoreOperation storeOperation,
            ValueStatus valStatus) {
            this.val = val;
            this.storeOperation = storeOperation;
            this.valStatus = valStatus;
        }

        /**
         * Added next value that waiting for a previous store operation.
         *
         * @param val Value.
         * @param storeOperation Store operation.
         */
        private void setNext(@Nullable Entry<? extends K, ? extends V> val,
            StoreOperation storeOperation) {
            this.nextVal = val;
            this.nextStoreOperation = storeOperation;
        }

        /**
         * Awaits a signal on flush condition.
         *
         * @throws IgniteInterruptedCheckedException If thread was interrupted.
         */
        private void waitForFlush() throws IgniteInterruptedCheckedException {
            U.await(flushCond);
        }

        /**
         * Signals flush condition.
         */
        @SuppressWarnings({"SignalWithoutCorrespondingAwait"})
        private void signalFlushed() {
            flushCond.signalAll();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof StatefulValue))
                return false;

            StatefulValue other = (StatefulValue)o;

            return F.eq(val, other.val) && F.eq(valStatus, other.valStatus);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val != null ? val.hashCode() : 0;

            res = 31 * res + valStatus.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StatefulValue.class, this);
        }
    }
}
