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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
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
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static javax.cache.Cache.Entry;

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

    /** Cache flush frequency. All pending operations will be performed in not less then this value ms. */
    private long cacheFlushFreq = CacheConfiguration.DFLT_WRITE_BEHIND_FLUSH_FREQUENCY;

    /** Maximum batch size for put and remove operations */
    private int batchSize = CacheConfiguration.DFLT_WRITE_BEHIND_BATCH_SIZE;

    /** Grid name. */
    private String gridName;

    /** Cache name. */
    private String cacheName;

    /** Underlying store. */
    private CacheStore<K, V> store;

    /** Write cache. */
    private ConcurrentLinkedHashMap<K, StatefulValue<K, V>> writeCache;

    /** Flusher threads. */
    private GridWorker[] flushThreads;

    /** Atomic flag indicating store shutdown. */
    private AtomicBoolean stopping = new AtomicBoolean(true);

    /** Flush lock. */
    private Lock flushLock = new ReentrantLock();

    /** Condition to determine records available for flush. */
    private Condition canFlush = flushLock.newCondition();

    /** Variable for counting total cache overflows. */
    private AtomicInteger cacheTotalOverflowCntr = new AtomicInteger();

    /** Variable contains current number of overflow events. */
    private AtomicInteger cacheOverflowCntr = new AtomicInteger();

    /** Variable for counting key-value pairs that are in {@link ValueStatus#RETRY} state. */
    private AtomicInteger retryEntriesCnt = new AtomicInteger();

    /** Log. */
    private IgniteLogger log;

    /** Store manager. */
    private CacheStoreManager storeMgr;

    /**
     * Creates a write-behind cache store for the given store.
     *
     * @param storeMgr Store manager.
     * @param gridName Grid name.
     * @param cacheName Cache name.
     * @param log Grid logger.
     * @param store {@code GridCacheStore} that need to be wrapped.
     */
    public GridCacheWriteBehindStore(
        CacheStoreManager storeMgr,
        String gridName,
        String cacheName,
        IgniteLogger log,
        CacheStore<K, V> store) {
        this.storeMgr = storeMgr;
        this.gridName = gridName;
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
     * {@link CacheConfiguration#DFLT_WRITE_BEHIND_CRITICAL_SIZE}
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
        return writeCache.sizex();
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

            flushThreads = new GridWorker[flushThreadCnt];

            writeCache = new ConcurrentLinkedHashMap<>(initCap, 0.75f, concurLvl);

            for (int i = 0; i < flushThreads.length; i++) {
                flushThreads[i] = new Flusher(gridName, "flusher-" + i, log);

                new IgniteThread(flushThreads[i]).start();
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

            wakeUp();

            boolean graceful = true;

            for (GridWorker worker : flushThreads)
                graceful &= U.join(worker, log);

            if (!graceful)
                log.warning("Shutdown was aborted");
        }
    }

    /**
     * Forces all entries collected to be flushed to the underlying store.
     * @throws IgniteCheckedException If failed.
     */
    public void forceFlush() throws IgniteCheckedException {
        wakeUp();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) {
        store.loadCache(clo, args);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        if (log.isDebugEnabled())
            log.debug("Store load all [keys=" + keys + ']');

        Map<K, V> loaded = new HashMap<>();

        Collection<K> remaining = new LinkedList<>();

        for (K key : keys) {
            StatefulValue<K, V> val = writeCache.get(key);

            if (val != null) {
                val.readLock().lock();

                try {
                    if (val.operation() == StoreOperation.PUT)
                        loaded.put(key, val.entry().getValue());
                    else
                        assert val.operation() == StoreOperation.RMV : val.operation();
                }
                finally {
                    val.readLock().unlock();
                }
            }
            else
                remaining.add(key);
        }

        // For items that were not found in queue.
        if (!remaining.isEmpty()) {
            Map<K, V> loaded0 = store.loadAll(remaining);

            if (loaded0 != null)
                loaded.putAll(loaded0);
        }

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public V load(K key) {
        if (log.isDebugEnabled())
            log.debug("Store load [key=" + key + ']');

        StatefulValue<K, V> val = writeCache.get(key);

        if (val != null) {
            val.readLock().lock();

            try {
                switch (val.operation()) {
                    case PUT:
                        return val.entry().getValue();

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
                log.debug("Store put [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

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
    @SuppressWarnings("unchecked")
    @Override public void delete(Object key) {
        try {
            if (log.isDebugEnabled())
                log.debug("Store remove [key=" + key + ']');

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
     * @param operation Updated value status
     * @throws IgniteInterruptedCheckedException If interrupted while waiting for value to be flushed.
     */
    private void updateCache(K key,
        @Nullable Entry<? extends K, ? extends V> val,
        StoreOperation operation)
        throws IgniteInterruptedCheckedException {
        StatefulValue<K, V> newVal = new StatefulValue<>(val, operation);

        StatefulValue<K, V> prev;

        while ((prev = writeCache.putIfAbsent(key, newVal)) != null) {
            prev.writeLock().lock();

            try {
                if (prev.status() == ValueStatus.PENDING) {
                    // Flush process in progress, try again.
                    prev.waitForFlush();

                    continue;
                }
                else if (prev.status() == ValueStatus.FLUSHED)
                    // This entry was deleted from map before we acquired the lock.
                    continue;
                else if (prev.status() == ValueStatus.RETRY)
                    // New value has come, old value is no longer in RETRY state,
                    retryEntriesCnt.decrementAndGet();

                assert prev.status() == ValueStatus.NEW || prev.status() == ValueStatus.RETRY;

                prev.update(val, operation, ValueStatus.NEW);

                break;
            }
            finally {
                prev.writeLock().unlock();
            }
        }

        // Now check the map size
        if (writeCache.sizex() > cacheCriticalSize)
            // Perform single store update in the same thread.
            flushSingleValue();
        else if (cacheMaxSize > 0 && writeCache.sizex() > cacheMaxSize)
            wakeUp();
    }

    /**
     * Flushes one upcoming value to the underlying store. Called from
     * {@link #updateCache(Object, Entry, StoreOperation)} method in case when current map size exceeds
     * critical size.
     */
    private void flushSingleValue() {
        cacheOverflowCntr.incrementAndGet();

        try {
            Map<K, StatefulValue<K, V>> batch = null;

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
                    applyBatch(batch, false);

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
     */
    private void applyBatch(Map<K, StatefulValue<K, V>> valMap, boolean initSes) {
        assert valMap.size() <= batchSize;

        StoreOperation operation = null;

        // Construct a map for underlying store
        Map<K, Entry<? extends K, ? extends V>> batch = U.newLinkedHashMap(valMap.size());

        for (Map.Entry<K, StatefulValue<K, V>> e : valMap.entrySet()) {
            if (operation == null)
                operation = e.getValue().operation();

            assert operation == e.getValue().operation();

            assert e.getValue().status() == ValueStatus.PENDING;

            batch.put(e.getKey(), e.getValue().entry());
        }

        if (updateStore(operation, batch, initSes)) {
            for (Map.Entry<K, StatefulValue<K, V>> e : valMap.entrySet()) {
                StatefulValue<K, V> val = e.getValue();

                val.writeLock().lock();

                try {
                    val.status(ValueStatus.FLUSHED);

                    StatefulValue<K, V> prev = writeCache.remove(e.getKey());

                    // Additional check to ensure consistency.
                    assert prev == val : "Map value for key " + e.getKey() + " was updated during flush";

                    val.signalFlushed();
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
                    val.status(ValueStatus.RETRY);

                    retryEntriesCnt.incrementAndGet();

                    val.signalFlushed();
                }
                finally {
                    val.writeLock().unlock();
                }
            }
        }
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
     * @return {@code true} if value may be deleted from the write cache,
     *         {@code false} otherwise
     */
    private boolean updateStore(StoreOperation operation,
        Map<K, Entry<? extends K, ? extends  V>> vals,
        boolean initSes) {

        try {
            if (initSes && storeMgr != null)
                storeMgr.writeBehindSessionInit();

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
            LT.error(log, e, "Unable to update underlying store: " + store);

            if (writeCache.sizex() > cacheCriticalSize || stopping.get()) {
                for (Map.Entry<K, Entry<? extends K, ? extends  V>> entry : vals.entrySet()) {
                    Object val = entry.getValue() != null ? entry.getValue().getValue() : null;

                    log.warning("Failed to update store (value will be lost as current buffer size is greater " +
                        "than 'cacheCriticalSize' or node has been stopped before store was repaired) [key=" +
                        entry.getKey() + ", val=" + val + ", op=" + operation + "]");
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
     * Thread that performs time-based flushing of written values to the underlying storage.
     */
    private class Flusher extends GridWorker {
        /** {@inheritDoc */
        protected Flusher(String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!stopping.get() || writeCache.sizex() > 0) {
                awaitOperationsAvailable();

                flushCache(writeCache.entrySet().iterator());
            }
        }

        /**
         * This method awaits until enough elements in map are available or given timeout is over.
         *
         * @throws InterruptedException If awaiting was interrupted.
         */
        private void awaitOperationsAvailable() throws InterruptedException {
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
         * Removes values from the write cache and performs corresponding operation
         * on the underlying store.
         *
         * @param it Iterator for write cache.
         */
        private void flushCache(Iterator<Map.Entry<K,StatefulValue<K, V>>> it) {
            StoreOperation operation = null;

            Map<K, StatefulValue<K, V>> batch = null;
            Map<K, StatefulValue<K, V>> pending  = U.newLinkedHashMap(batchSize);

            while (it.hasNext()) {
                Map.Entry<K, StatefulValue<K, V>> e = it.next();

                StatefulValue<K, V> val = e.getValue();

                val.writeLock().lock();

                try {
                    ValueStatus status = val.status();

                    if (acquired(status))
                        // Another thread is helping us, continue to the next entry.
                        continue;

                    if (status == ValueStatus.RETRY)
                        retryEntriesCnt.decrementAndGet();

                    assert retryEntriesCnt.get() >= 0;

                    val.status(ValueStatus.PENDING);

                    // We scan for the next operation and apply batch on operation change. Null means new batch.
                    if (operation == null)
                        operation = val.operation();

                    if (operation != val.operation()) {
                        // Operation is changed, so we need to perform a batch.
                        batch = pending;
                        pending = U.newLinkedHashMap(batchSize);

                        operation = val.operation();

                        pending.put(e.getKey(), val);
                    }
                    else
                        pending.put(e.getKey(), val);

                    if (pending.size() == batchSize) {
                        batch = pending;
                        pending = U.newLinkedHashMap(batchSize);

                        operation = null;
                    }
                }
                finally {
                    val.writeLock().unlock();
                }

                if (batch != null && !batch.isEmpty()) {
                    applyBatch(batch, true);
                    batch = null;
                }
            }

            // Process the remainder.
            if (!pending.isEmpty())
                applyBatch(pending, true);
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

        /** Store operation has failed and it will be re-tried at the next flush. */
        RETRY,

        /** Store operation succeeded and this value will be removed by flusher. */
        FLUSHED,
    }

    /**
     * Checks if given status indicates pending or complete flush operation.
     *
     * @param status Status to check.
     * @return {@code true} if status indicates any pending or complete store update operation.
     */
    private boolean acquired(ValueStatus status) {
        return status == ValueStatus.PENDING || status == ValueStatus.FLUSHED;
    }

    /**
     * A state-value-operation trio.
     *
     * @param <V> Value type.
     */
    private static class StatefulValue<K, V> extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Value. */
        @GridToStringInclude(sensitive = true)
        private Entry<? extends K, ? extends V> val;

        /** Store operation. */
        private StoreOperation storeOperation;

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
         * @return Value status
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
         * Awaits a signal on flush condition
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