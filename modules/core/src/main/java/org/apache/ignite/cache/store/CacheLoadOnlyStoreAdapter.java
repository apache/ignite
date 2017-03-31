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

package org.apache.ignite.cache.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This adapter designed to support stores with bulk loading from stream-like source.
 * <p>
 * This class processes input data in the following way:
 * <ul>
 *      <li>
 *          Iterator of input record obtained from user-defined {@link #inputIterator(Object...)}.
 *      </li>
 *      <li>
 *          Iterator continuously queried for input records and they are grouped into batches of {@link #batchSize}.
 *      </li>
 *      <li>
 *          Batch is placed into processing queue and puled by one of {@link #threadsCnt} working threads.
 *      </li>
 *      <li>
 *          Each record in batch is passed to user-defined {@link #parse(Object, Object...)} method
 *          and result is stored into cache.
 *      </li>
 * </ul>
 * <p>
 * Two methods should be implemented by inheritants:
 * <ul>
 *      <li>
 *          {@link #inputIterator(Object...)}. It should open underlying data source
 *          and iterate all record available in it. Individual records could be in very raw form,
 *          like text lines for CSV files.
 *      </li>
 *      <li>
 *          {@link #parse(Object, Object...)}. This method should process input records
 *          and transform them into key-value pairs for cache.
 *      </li>
 * </ul>
 * <p>
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @param <I> Input type.
 */
public abstract class CacheLoadOnlyStoreAdapter<K, V, I> implements CacheStore<K, V> {
    /**
     * Default batch size (number of records read with {@link #inputIterator(Object...)}
     * and then submitted to internal pool at a time).
     */
    public static final int DFLT_BATCH_SIZE = 100;

    /** Default batch queue size (max batches count to limit memory usage). */
    public static final int DFLT_BATCH_QUEUE_SIZE = 100;

    /** Default number of working threads (equal to the number of available processors). */
    public static final int DFLT_THREADS_COUNT = Runtime.getRuntime().availableProcessors();

    /** Auto-injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Batch size. */
    private int batchSize = DFLT_BATCH_SIZE;

    /** Size of queue of batches to process. */
    private int batchQueueSize = DFLT_BATCH_QUEUE_SIZE;

    /** Number fo working threads. */
    private int threadsCnt = DFLT_THREADS_COUNT;

    /**
     * Returns iterator of input records.
     * <p>
     * Note that returned iterator doesn't have to be thread-safe. Thus it could
     * operate on raw streams, DB connections, etc. without additional synchronization.
     *
     * @param args Arguments passes into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Iterator over input records.
     * @throws CacheLoaderException If iterator can't be created with the given arguments.
     */
    protected abstract Iterator<I> inputIterator(@Nullable Object... args) throws CacheLoaderException;

    /**
     * This method should transform raw data records into valid key-value pairs
     * to be stored into cache.
     * <p>
     * If {@code null} is returned then this record will be just skipped.
     *
     * @param rec A raw data record.
     * @param args Arguments passed into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
     * @return Cache entry to be saved in cache or {@code null} if no entry could be produced from this record.
     */
    @Nullable protected abstract IgniteBiTuple<K, V> parse(I rec, @Nullable Object... args);

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> c, @Nullable Object... args) {
        ExecutorService exec = new ThreadPoolExecutor(
            threadsCnt,
            threadsCnt,
            0L,
            MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(batchQueueSize),
            new BlockingRejectedExecutionHandler());

        Iterator<I> iter = inputIterator(args);

        Collection<I> buf = new ArrayList<>(batchSize);

        try {
            while (iter.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    U.warn(log, "Working thread was interrupted while loading data.");

                    break;
                }

                buf.add(iter.next());

                if (buf.size() == batchSize) {
                    exec.execute(new Worker(c, buf, args));

                    buf = new ArrayList<>(batchSize);
                }
            }

            if (!buf.isEmpty())
                exec.execute(new Worker(c, buf, args));
        }
        catch (RejectedExecutionException ignored) {
            // Because of custom RejectedExecutionHandler.
            assert false : "RejectedExecutionException was thrown while it shouldn't.";
        }
        finally {
            exec.shutdown();

            try {
                exec.awaitTermination(Long.MAX_VALUE, MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                U.warn(log, "Working thread was interrupted while waiting for put operations to complete.");

                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Returns batch size.
     *
     * @return Batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets batch size.
     *
     * @param batchSize Batch size.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Returns batch queue size.
     *
     * @return Batch queue size.
     */
    public int getBatchQueueSize() {
        return batchQueueSize;
    }

    /**
     * Sets batch queue size.
     *
     * @param batchQueueSize Batch queue size.
     */
    public void setBatchQueueSize(int batchQueueSize) {
        this.batchQueueSize = batchQueueSize;
    }

    /**
     * Returns number of worker threads.
     *
     * @return Number of worker threads.
     */
    public int getThreadsCount() {
        return threadsCnt;
    }

    /**
     * Sets number of worker threads.
     *
     * @param threadsCnt Number of worker threads.
     */
    public void setThreadsCount(int threadsCnt) {
        this.threadsCnt = threadsCnt;
    }

    /** {@inheritDoc} */
    @Override public V load(K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }

    /**
     * Worker.
     */
    private class Worker implements Runnable {
        /** */
        private final IgniteBiInClosure<K, V> c;

        /** */
        private final Collection<I> buf;

        /** */
        private final Object[] args;

        /**
         * @param c Closure for loaded entries.
         * @param buf Set of input records to process.
         * @param args Arguments passed into {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
         */
        Worker(IgniteBiInClosure<K, V> c, Collection<I> buf, Object[] args) {
            this.c = c;
            this.buf = buf;
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (I rec : buf) {
                IgniteBiTuple<K, V> entry = parse(rec, args);

                if (entry != null)
                    c.apply(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * This handler blocks the caller thread until free space will be available in tasks queue.
     * If the executor is shut down than it throws {@link RejectedExecutionException}.
     * <p>
     * It is save to apply this policy when:
     * <ol>
     *      <li>{@code shutdownNow} is not used on the pool.</li>
     *      <li>{@code shutdown} is called from the thread where all submissions where performed.</li>
     * </ol>
     */
    private class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {
        /** {@inheritDoc} */
        @Override public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                if (executor.isShutdown())
                    throw new RejectedExecutionException();
                else
                    executor.getQueue().put(r);
            }
            catch (InterruptedException ignored) {
                U.warn(log, "Working thread was interrupted while loading data.");

                Thread.currentThread().interrupt();
            }
        }
    }
}
