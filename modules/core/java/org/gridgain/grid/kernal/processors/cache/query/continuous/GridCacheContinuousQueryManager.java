// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Continuous queries manager.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheContinuousQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Ordered topic prefix. */
    private static final String TOPIC_PREFIX = "CONTINUOUS_QUERY";

    /** Force unwind interval. */
    private static final long FORCE_UNWIND_INTERVAL = 1000;

    /** Maximum buffer size. */
    private int maxBufSize;

    /** Listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Query sequence number for message topic. */
    private final AtomicLong seq = new AtomicLong();

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Threads. */
    private final Collection<GridThread> threads = new HashSet<>();

    /** Buffer. */
    private volatile ConcurrentLinkedDeque8<GridCacheContinuousQueryEntry<K, V>> buf =
        new ConcurrentLinkedDeque8<>();

    /** Queue. */
    private BlockingQueue<Queue<GridCacheContinuousQueryEntry<K, V>>> queue;

    /** {@inheritDoc} */
    @Override protected void start0() throws GridException {
        GridCacheConfiguration cfg = cctx.config();

        maxBufSize = cfg.getContinuousQueryMaximumBufferSize();

        queue = new LinkedBlockingQueue<>(cfg.getContinuousQueryQueueSize() / maxBufSize);

        threads.add(new GridThread(new GridWorker(cctx.gridName(), "continuous-query-notifier", log) {
            @Override protected void body() {
                while (!isCancelled()) {
                    Queue<GridCacheContinuousQueryEntry<K, V>> q;

                    try {
                        q = queue.take();
                    }
                    catch (InterruptedException ignored) {
                        break;
                    }

                    GridCacheContinuousQueryEntry<K, V> e;

                    while ((e = q.poll()) != null) {
                        for (ListenerInfo<K, V> lsnr : lsnrs.values())
                            lsnr.onEntryUpdate(e);
                    }
                }
            }
        }));

        threads.add(new GridThread(new GridWorker(cctx.gridName(), "continuous-query-unwinder", log) {
            @Override protected void body() {
                while (!isCancelled()) {
                    try {
                        U.sleep(FORCE_UNWIND_INTERVAL);
                    }
                    catch (GridInterruptedException ignored) {
                        break;
                    }

                    unwind(true);
                }
            }
        }));

        U.startThreads(threads);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        U.interrupt(threads);
        U.joinThreads(threads, log);
    }

    /**
     * @param prjPred Projection predicate.
     * @return New continuous query.
     */
    public GridCacheContinuousQuery<K, V> createQuery(@Nullable GridPredicate<? super GridCacheEntry<K, V>> prjPred) {
        Object topic = TOPIC_CACHE.topic(TOPIC_PREFIX, cctx.localNodeId(), seq.getAndIncrement());

        return new GridCacheContinuousQueryAdapter<>(cctx, topic, prjPred);
    }

    /**
     * @param e Cache entry.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param unwind Whether to unwind after entry is added to buffer.
     * @throws GridException In case of error.
     */
    public void onEntryUpdate(GridCacheEntryEx<K, V> e, K key, @Nullable V val, @Nullable GridCacheValueBytes valBytes,
        boolean unwind) throws GridException {
        assert e != null;
        assert key != null;

        if (lsnrCnt.get() > 0) {
            if (e.isInternal())
                return;

            GridCacheContinuousQueryEntry<K, V> e0 =
                new GridCacheContinuousQueryEntry<>(cctx, e.wrap(false), key, val, valBytes);

            e0.initValue(cctx.marshaller(), cctx.deploy().globalLoader());

            if (unwind && buf.size() >= maxBufSize - 1) {
                lock.writeLock().lock();

                try {
                    buf.add(e0);

                    unwind0();
                }
                finally {
                    lock.writeLock().unlock();
                }
            }
            else {
                lock.readLock().lock();

                try {
                    buf.add(e0);
                }
                finally {
                    lock.readLock().unlock();
                }
            }
        }
    }

    /**
     * Unwinds entries buffer.
     *
     * @param force Whether to unwind regardless of current buffer size.
     */
    public void unwind(boolean force) {
        if (force || buf.sizex() >= maxBufSize) {
            lock.writeLock().lock();

            try {
                unwind0();
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Unwinds entries buffer.
     */
    private void unwind0() {
        assert lock.isWriteLockedByCurrentThread();

        try {
            queue.put(buf);
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        buf = new ConcurrentLinkedDeque8<>();
    }

    /**
     * @return Current buffer size.
     */
    public int currentQueueSize() {
        return maxBufSize * queue.size() + buf.sizex();
    }

    /**
     * @param id Listener ID.
     * @param lsnr Listener.
     * @return Whether listener was actually registered.
     */
    boolean registerListener(UUID id, GridCacheContinuousQueryListener<K, V> lsnr) {
        ListenerInfo<K, V> info = new ListenerInfo<>(lsnr);

        boolean added = lsnrs.putIfAbsent(id, info) == null;

        if (added)
            lsnrCnt.incrementAndGet();

        return added;
    }

    /**
     * Iterates through existing data.
     *
     * @param id Listener ID.
     */
    void iterate(UUID id) {
        ListenerInfo<K, V> info = lsnrs.get(id);

        assert info != null;

        for (GridCacheEntry<K, V> e : cctx.cache().primaryEntrySet())
            info.onIterate(new GridCacheContinuousQueryEntry<>(cctx, e, e.getKey(), e.getValue(), null));

        info.flushPending();
    }

    /**
     * @param id Listener ID.
     */
    void unregisterListener(UUID id) {
        if (lsnrs.remove(id) != null)
            lsnrCnt.decrementAndGet();
    }

    /**
     * Listener info.
     */
    private static class ListenerInfo<K, V> {
        /** Listener. */
        private final GridCacheContinuousQueryListener<K, V> lsnr;

        /** Pending entries. */
        private Collection<GridCacheContinuousQueryEntry<K, V>> pending = new LinkedList<>();

        /**
         * @param lsnr Listener.
         */
        private ListenerInfo(GridCacheContinuousQueryListener<K, V> lsnr) {
            this.lsnr = lsnr;
        }

        /**
         * @param e Entry update callback.
         */
        void onEntryUpdate(GridCacheContinuousQueryEntry<K, V> e) {
            boolean notifyLsnr = true;

            synchronized (this) {
                if (pending != null) {
                    pending.add(e);

                    notifyLsnr = false;
                }
            }

            if (notifyLsnr)
                lsnr.onEntryUpdate(e);
        }

        /**
         * @param e Entry iteration callback.
         */
        @SuppressWarnings("TypeMayBeWeakened")
        void onIterate(GridCacheContinuousQueryEntry<K, V> e) {
            lsnr.onEntryUpdate(e);
        }

        /**
         * Flushes pending entries to listener.
         */
        void flushPending() {
            Collection<GridCacheContinuousQueryEntry<K, V>> pending0;

            synchronized (this) {
                pending0 = pending;

                pending = null;
            }

            for (GridCacheContinuousQueryEntry<K, V> e : pending0)
                lsnr.onEntryUpdate(e);
        }
    }
}
