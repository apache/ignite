/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * This adepter designed to support stores with bulk loading from stream-like source.
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
public abstract class GridCacheLoadOnlyStoreAdapter<K, V, I> implements GridCacheStore<K, V> {
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
    @GridLoggerResource
    private GridLogger log;

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
     * @param args Arguments passes into {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)} method.
     * @return Iterator over input records.
     * @throws GridException If iterator can't be created with the given arguments.
     */
    protected abstract Iterator<I> inputIterator(@Nullable Object... args) throws GridException;

    /**
     * This method should transform raw data records into valid key-value pairs
     * to be stored into cache.
     * <p>
     * If {@code null} is returned then this record will be just skipped.
     *
     * @param rec A raw data record.
     * @param args Arguments passed into {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)} method.
     * @return Cache entry to be saved in cache or {@code null} if no entry could be produced from this record.
     */
    @Nullable protected abstract IgniteBiTuple<K, V> parse(I rec, @Nullable Object... args);

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> c, @Nullable Object... args)
        throws GridException {
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
                    exec.submit(new Worker(c, buf, args));

                    buf = new ArrayList<>(batchSize);
                }
            }

            if (!buf.isEmpty())
                exec.submit(new Worker(c, buf, args));
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
    @Override public V load(@Nullable GridCacheTx tx, K key)
        throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable GridCacheTx tx,
        @Nullable Collection<? extends K> keys, IgniteBiInClosure<K, V> c) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, K key, @Nullable V val) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable GridCacheTx tx, @Nullable Map<? extends K, ? extends V> map)
        throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, K key) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridCacheTx tx, @Nullable Collection<? extends K> keys)
        throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
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
         * @param args Arguments passed into {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)} method.
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
