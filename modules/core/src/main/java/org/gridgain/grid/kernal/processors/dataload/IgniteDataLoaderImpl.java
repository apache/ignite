/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.dataload.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 * Data loader implementation.
 */
public class IgniteDataLoaderImpl<K, V> implements IgniteDataLoader<K, V>, Delayed {
    /** */
    public static final GridProductVersion COMPACT_MAP_ENTRIES_SINCE = GridProductVersion.fromString("6.5.6");

    /** Cache updater. */
    private IgniteDataLoadCacheUpdater<K, V> updater = GridDataLoadCacheUpdaters.individual();

    /** */
    private byte[] updaterBytes;

    /** Max remap count before issuing an error. */
    private static final int MAX_REMAP_CNT = 32;

    /** Log reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Cache name ({@code null} for default cache). */
    private final String cacheName;

    /** Portable enabled flag. */
    private final boolean portableEnabled;

    /**
     *  If {@code true} then data will be transferred in compact format (only keys and values).
     *  Otherwise full map entry will be transferred (this is requires by DR internal logic).
     */
    private final boolean compact;

    /** Per-node buffer size. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private int bufSize = DFLT_PER_NODE_BUFFER_SIZE;

    /** */
    private int parallelOps = DFLT_MAX_PARALLEL_OPS;

    /** */
    private long autoFlushFreq;

    /** Mapping. */
    @GridToStringInclude
    private ConcurrentMap<UUID, Buffer> bufMappings = new ConcurrentHashMap8<>();

    /** Logger. */
    private GridLogger log;

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr;

    /** Context. */
    private final GridKernalContext ctx;

    /** Communication topic for responses. */
    private final Object topic;

    /** */
    private byte[] topicBytes;

    /** {@code True} if data loader has been cancelled. */
    private volatile boolean cancelled;

    /** Active futures of this data loader. */
    @GridToStringInclude
    private final Collection<IgniteFuture<?>> activeFuts = new GridConcurrentHashSet<>();

    /** Closure to remove from active futures. */
    @GridToStringExclude
    private final IgniteInClosure<IgniteFuture<?>> rmvActiveFut = new IgniteInClosure<IgniteFuture<?>>() {
        @Override public void apply(IgniteFuture<?> t) {
            boolean rmv = activeFuts.remove(t);

            assert rmv;
        }
    };

    /** Job peer deploy aware. */
    private volatile GridPeerDeployAware jobPda;

    /** Deployment class. */
    private Class<?> depCls;

    /** Future to track loading finish. */
    private final GridFutureAdapter<?> fut;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private volatile long lastFlushTime = U.currentTimeMillis();

    /** */
    private final DelayQueue<IgniteDataLoaderImpl<K, V>> flushQ;

    /**
     * @param ctx Grid kernal context.
     * @param cacheName Cache name.
     * @param flushQ Flush queue.
     * @param compact If {@code true} data is transferred in compact mode (only keys and values).
     *                Otherwise full map entry will be transferred (this is required by DR internal logic).
     */
    public IgniteDataLoaderImpl(
        final GridKernalContext ctx,
        @Nullable final String cacheName,
        DelayQueue<IgniteDataLoaderImpl<K, V>> flushQ,
        boolean compact
    ) {
        assert ctx != null;

        this.ctx = ctx;
        this.cacheName = cacheName;
        this.flushQ = flushQ;
        this.compact = compact;

        log = U.logger(ctx, logRef, IgniteDataLoaderImpl.class);

        ClusterNode node = F.first(ctx.grid().forCache(cacheName).nodes());

        if (node == null)
            throw new IllegalStateException("Cache doesn't exist: " + cacheName);

        Map<String, Boolean> attrPortable = node.attribute(ATTR_CACHE_PORTABLE);

        Boolean portableEnabled0 = attrPortable == null ? null : attrPortable.get(CU.mask(cacheName));

        portableEnabled = portableEnabled0 == null ? false : portableEnabled0;

        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(IgniteEvent evt) {
                assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                IgniteDiscoveryEvent discoEvt = (IgniteDiscoveryEvent)evt;

                UUID id = discoEvt.eventNode().id();

                // Remap regular mappings.
                final Buffer buf = bufMappings.remove(id);

                if (buf != null) {
                    // Only async notification is possible since
                    // discovery thread may be trapped otherwise.
                    ctx.closure().callLocalSafe(
                        new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                buf.onNodeLeft();

                                return null;
                            }
                        },
                        true /* system pool */
                    );
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

        // Generate unique topic for this loader.
        topic = TOPIC_DATALOAD.topic(IgniteUuid.fromUuid(ctx.localNodeId()));

        ctx.io().addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert msg instanceof GridDataLoadResponse;

                GridDataLoadResponse res = (GridDataLoadResponse)msg;

                if (log.isDebugEnabled())
                    log.debug("Received data load response: " + res);

                Buffer buf = bufMappings.get(nodeId);

                if (buf != null)
                    buf.onResponse(res);

                else if (log.isDebugEnabled())
                    log.debug("Ignoring response since node has left [nodeId=" + nodeId + ", ");
            }
        });

        if (log.isDebugEnabled())
            log.debug("Added response listener within topic: " + topic);

        fut = new GridDataLoaderFuture(ctx, this);
    }

    /**
     * Enters busy lock.
     */
    private void enterBusy() {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Data loader has been closed.");
    }

    /**
     * Leaves busy lock.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public void deployClass(Class<?> depCls) {
        this.depCls = depCls;
    }

    /** {@inheritDoc} */
    @Override public void updater(IgniteDataLoadCacheUpdater<K, V> updater) {
        A.notNull(updater, "updater");

        this.updater = updater;
    }

    /** {@inheritDoc} */
    @Override public boolean isolated() {
        return updater != GridDataLoadCacheUpdaters.individual();
    }

    /** {@inheritDoc} */
    @Override public void isolated(boolean isolated) throws GridException {
        if (isolated())
            return;

        ClusterNode node = F.first(ctx.grid().forCache(cacheName).nodes());

        if (node == null)
            throw new GridException("Failed to get node for cache: " + cacheName);

        GridCacheAttributes a = U.cacheAttributes(node, cacheName);

        assert a != null;

        updater = a.atomicityMode() == GridCacheAtomicityMode.ATOMIC ?
            GridDataLoadCacheUpdaters.<K, V>batched() :
            GridDataLoadCacheUpdaters.<K, V>groupLocked();
    }

    /** {@inheritDoc} */
    @Override @Nullable public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public int perNodeBufferSize() {
        return bufSize;
    }

    /** {@inheritDoc} */
    @Override public void perNodeBufferSize(int bufSize) {
        A.ensure(bufSize > 0, "bufSize > 0");

        this.bufSize = bufSize;
    }

    /** {@inheritDoc} */
    @Override public int perNodeParallelLoadOperations() {
        return parallelOps;
    }

    /** {@inheritDoc} */
    @Override public void perNodeParallelLoadOperations(int parallelOps) {
        this.parallelOps = parallelOps;
    }

    /** {@inheritDoc} */
    @Override public long autoFlushFrequency() {
        return autoFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public void autoFlushFrequency(long autoFlushFreq) {
        A.ensure(autoFlushFreq >= 0, "autoFlushFreq >= 0");

        long old = this.autoFlushFreq;

        if (autoFlushFreq != old) {
            this.autoFlushFreq = autoFlushFreq;

            if (autoFlushFreq != 0 && old == 0)
                flushQ.add(this);
            else if (autoFlushFreq == 0)
                flushQ.remove(this);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Map<K, V> entries) throws IllegalStateException {
        A.notNull(entries, "entries");

        return addData(entries.entrySet());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries) {
        A.notEmpty(entries, "entries");

        enterBusy();

        try {
            GridFutureAdapter<Object> resFut = new GridFutureAdapter<>(ctx);

            activeFuts.add(resFut);

            resFut.listenAsync(rmvActiveFut);

            Collection<K> keys = new GridConcurrentHashSet<>(entries.size(), 1.0f, 16);

            for (Map.Entry<K, V> entry : entries)
                keys.add(entry.getKey());

            load0(entries, resFut, keys, 0);

            return resFut;
        }
        catch (GridRuntimeException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Map.Entry<K, V> entry) throws GridException, IllegalStateException {
        A.notNull(entry, "entry");

        return addData(F.asList(entry));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(K key, V val) throws GridException, IllegalStateException {
        A.notNull(key, "key");

        return addData(new Entry0<>(key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeData(K key) throws GridException, IllegalStateException {
        return addData(key, null);
    }

    /**
     * @param entries Entries.
     * @param resFut Result future.
     * @param activeKeys Active keys.
     * @param remaps Remaps count.
     */
    private void load0(
        Collection<? extends Map.Entry<K, V>> entries,
        final GridFutureAdapter<Object> resFut,
        final Collection<K> activeKeys,
        final int remaps
    ) {
        assert entries != null;

        if (remaps >= MAX_REMAP_CNT) {
            resFut.onDone(new GridException("Failed to finish operation (too many remaps): " + remaps));

            return;
        }

        Map<ClusterNode, Collection<Map.Entry<K, V>>> mappings = new HashMap<>();

        boolean initPda = ctx.deploy().enabled() && jobPda == null;

        for (Map.Entry<K, V> entry : entries) {
            ClusterNode node;

            try {
                K key = entry.getKey();

                assert key != null;

                if (initPda) {
                    jobPda = new DataLoaderPda(key, entry.getValue(), updater);

                    initPda = false;
                }

                node = ctx.affinity().mapKeyToNode(cacheName, key);
            }
            catch (GridException e) {
                resFut.onDone(e);

                return;
            }

            if (node == null) {
                resFut.onDone(new GridTopologyException("Failed to map key to node " +
                    "(no nodes with cache found in topology) [infos=" + entries.size() +
                    ", cacheName=" + cacheName + ']'));

                return;
            }

            Collection<Map.Entry<K, V>> col = mappings.get(node);

            if (col == null)
                mappings.put(node, col = new ArrayList<>());

            col.add(entry);
        }

        for (final Map.Entry<ClusterNode, Collection<Map.Entry<K, V>>> e : mappings.entrySet()) {
            final UUID nodeId = e.getKey().id();

            Buffer buf = bufMappings.get(nodeId);

            if (buf == null) {
                Buffer old = bufMappings.putIfAbsent(nodeId, buf = new Buffer(e.getKey()));

                if (old != null)
                    buf = old;
            }

            final Collection<Map.Entry<K, V>> entriesForNode = e.getValue();

            IgniteInClosure<IgniteFuture<?>> lsnr = new IgniteInClosure<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> t) {
                    try {
                        t.get();

                        for (Map.Entry<K, V> e : entriesForNode)
                            activeKeys.remove(e.getKey());

                        if (activeKeys.isEmpty())
                            resFut.onDone();
                    }
                    catch (GridException e1) {
                        if (log.isDebugEnabled())
                            log.debug("Future finished with error [nodeId=" + nodeId + ", err=" + e1 + ']');

                        if (cancelled) {
                            resFut.onDone(new GridException("Data loader has been cancelled: " +
                                IgniteDataLoaderImpl.this, e1));
                        }
                        else
                            load0(entriesForNode, resFut, activeKeys, remaps + 1);
                    }
                }
            };

            GridFutureAdapter<?> f;

            try {
                f = buf.update(entriesForNode, lsnr);
            }
            catch (GridInterruptedException e1) {
                resFut.onDone(e1);

                return;
            }

            if (ctx.discovery().node(nodeId) == null) {
                if (bufMappings.remove(nodeId, buf))
                    buf.onNodeLeft();

                if (f != null)
                    f.onDone(new GridTopologyException("Failed to wait for request completion " +
                        "(node has left): " + nodeId));
            }
        }
    }

    /**
     * Performs flush.
     *
     * @throws GridException If failed.
     */
    private void doFlush() throws GridException {
        lastFlushTime = U.currentTimeMillis();

        List<IgniteFuture> activeFuts0 = null;

        int doneCnt = 0;

        for (IgniteFuture<?> f : activeFuts) {
            if (!f.isDone()) {
                if (activeFuts0 == null)
                    activeFuts0 = new ArrayList<>((int)(activeFuts.size() * 1.2));

                activeFuts0.add(f);
            }
            else {
                f.get();

                doneCnt++;
            }
        }

        if (activeFuts0 == null || activeFuts0.isEmpty())
            return;

        while (true) {
            Queue<IgniteFuture<?>> q = null;

            for (Buffer buf : bufMappings.values()) {
                IgniteFuture<?> flushFut = buf.flush();

                if (flushFut != null) {
                    if (q == null)
                        q = new ArrayDeque<>(bufMappings.size() * 2);

                    q.add(flushFut);
                }
            }

            if (q != null) {
                assert !q.isEmpty();

                boolean err = false;

                for (IgniteFuture fut = q.poll(); fut != null; fut = q.poll()) {
                    try {
                        fut.get();
                    }
                    catch (GridException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to flush buffer: " + e);

                        err = true;
                    }
                }

                if (err)
                    // Remaps needed - flush buffers.
                    continue;
            }

            doneCnt = 0;

            for (int i = 0; i < activeFuts0.size(); i++) {
                IgniteFuture f = activeFuts0.get(i);

                if (f == null)
                    doneCnt++;
                else if (f.isDone()) {
                    f.get();

                    doneCnt++;

                    activeFuts0.set(i, null);
                }
                else
                    break;
            }

            if (doneCnt == activeFuts0.size())
                return;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void flush() throws GridException {
        enterBusy();

        try {
            doFlush();
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Flushes every internal buffer if buffer was flushed before passed in
     * threshold.
     * <p>
     * Does not wait for result and does not fail on errors assuming that this method
     * should be called periodically.
     */
    @Override public void tryFlush() throws GridInterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            for (Buffer buf : bufMappings.values())
                buf.flush();

            lastFlushTime = U.currentTimeMillis();
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @throws GridException If failed.
     */
    @Override public void close(boolean cancel) throws GridException {
        if (!closed.compareAndSet(false, true))
            return;

        busyLock.block();

        if (log.isDebugEnabled())
            log.debug("Closing data loader [ldr=" + this + ", cancel=" + cancel + ']');

        GridException e = null;

        try {
            // Assuming that no methods are called on this loader after this method is called.
            if (cancel) {
                cancelled = true;

                for (Buffer buf : bufMappings.values())
                    buf.cancelAll();
            }
            else
                doFlush();

            ctx.event().removeLocalEventListener(discoLsnr);

            ctx.io().removeMessageListener(topic);
        }
        catch (GridException e0) {
            e = e0;
        }

        fut.onDone(null, e);

        if (e != null)
            throw e;
    }

    /**
     * @return {@code true} If the loader is closed.
     */
    boolean isClosed() {
        return fut.isDone();
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        close(false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDataLoaderImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public long getDelay(TimeUnit unit) {
        return unit.convert(nextFlushTime() - U.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @return Next flush time.
     */
    private long nextFlushTime() {
        return lastFlushTime + autoFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Delayed o) {
        return nextFlushTime() > ((IgniteDataLoaderImpl)o).nextFlushTime() ? 1 : -1;
    }

    /**
     *
     */
    private class Buffer {
        /** Node. */
        private final ClusterNode node;

        /** Active futures. */
        private final Collection<IgniteFuture<Object>> locFuts;

        /** Buffered entries. */
        private List<Map.Entry<K, V>> entries;

        /** */
        @GridToStringExclude
        private GridFutureAdapter<Object> curFut;

        /** Local node flag. */
        private final boolean isLocNode;

        /** ID generator. */
        private final AtomicLong idGen = new AtomicLong();

        /** Active futures. */
        private final ConcurrentMap<Long, GridFutureAdapter<Object>> reqs;

        /** */
        private final Semaphore sem;

        /** Closure to signal on task finish. */
        @GridToStringExclude
        private final IgniteInClosure<IgniteFuture<Object>> signalC = new IgniteInClosure<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> t) {
                signalTaskFinished(t);
            }
        };

        /**
         * @param node Node.
         */
        Buffer(ClusterNode node) {
            assert node != null;

            this.node = node;

            locFuts = new GridConcurrentHashSet<>();
            reqs = new ConcurrentHashMap8<>();

            // Cache local node flag.
            isLocNode = node.equals(ctx.discovery().localNode());

            entries = newEntries();
            curFut = new GridFutureAdapter<>(ctx);
            curFut.listenAsync(signalC);

            sem = new Semaphore(parallelOps);
        }

        /**
         * @param newEntries Infos.
         * @param lsnr Listener for the operation future.
         * @throws GridInterruptedException If failed.
         * @return Future for operation.
         */
        @Nullable GridFutureAdapter<?> update(Iterable<Map.Entry<K, V>> newEntries,
            IgniteInClosure<IgniteFuture<?>> lsnr) throws GridInterruptedException {
            List<Map.Entry<K, V>> entries0 = null;
            GridFutureAdapter<Object> curFut0;

            synchronized (this) {
                curFut0 = curFut;

                curFut0.listenAsync(lsnr);

                for (Map.Entry<K, V> entry : newEntries)
                    entries.add(entry);

                if (entries.size() >= bufSize) {
                    entries0 = entries;

                    entries = newEntries();
                    curFut = new GridFutureAdapter<>(ctx);
                    curFut.listenAsync(signalC);
                }
            }

            if (entries0 != null) {
                submit(entries0, curFut0);

                if (cancelled)
                    curFut0.onDone(new GridException("Data loader has been cancelled: " + IgniteDataLoaderImpl.this));
            }

            return curFut0;
        }

        /**
         * @return Fresh collection with some space for outgrowth.
         */
        private List<Map.Entry<K, V>> newEntries() {
            return new ArrayList<>((int)(bufSize * 1.2));
        }

        /**
         * @return Future if any submitted.
         *
         * @throws GridInterruptedException If thread has been interrupted.
         */
        @Nullable
        IgniteFuture<?> flush() throws GridInterruptedException {
            List<Map.Entry<K, V>> entries0 = null;
            GridFutureAdapter<Object> curFut0 = null;

            synchronized (this) {
                if (!entries.isEmpty()) {
                    entries0 = entries;
                    curFut0 = curFut;

                    entries = newEntries();
                    curFut = new GridFutureAdapter<>(ctx);
                    curFut.listenAsync(signalC);
                }
            }

            if (entries0 != null)
                submit(entries0, curFut0);

            // Create compound future for this flush.
            GridCompoundFuture<Object, Object> res = null;

            for (IgniteFuture<Object> f : locFuts) {
                if (res == null)
                    res = new GridCompoundFuture<>(ctx);

                res.add(f);
            }

            for (IgniteFuture<Object> f : reqs.values()) {
                if (res == null)
                    res = new GridCompoundFuture<>(ctx);

                res.add(f);
            }

            if (res != null)
                res.markInitialized();

            return res;
        }

        /**
         * Increments active tasks count.
         *
         * @throws GridInterruptedException If thread has been interrupted.
         */
        private void incrementActiveTasks() throws GridInterruptedException {
            U.acquire(sem);
        }

        /**
         * @param f Future that finished.
         */
        private void signalTaskFinished(IgniteFuture<Object> f) {
            assert f != null;

            sem.release();
        }

        /**
         * @param entries Entries to submit.
         * @param curFut Current future.
         * @throws GridInterruptedException If interrupted.
         */
        private void submit(final Collection<Map.Entry<K, V>> entries, final GridFutureAdapter<Object> curFut)
            throws GridInterruptedException {
            assert entries != null;
            assert !entries.isEmpty();
            assert curFut != null;

            incrementActiveTasks();

            IgniteFuture<Object> fut;

            if (isLocNode) {
                fut = ctx.closure().callLocalSafe(
                    new GridDataLoadUpdateJob<>(ctx, log, cacheName, entries, false, updater), false);

                locFuts.add(fut);

                fut.listenAsync(new IgniteInClosure<IgniteFuture<Object>>() {
                    @Override public void apply(IgniteFuture<Object> t) {
                        try {
                            boolean rmv = locFuts.remove(t);

                            assert rmv;

                            curFut.onDone(t.get());
                        }
                        catch (GridException e) {
                            curFut.onDone(e);
                        }
                    }
                });
            }
            else {
                byte[] entriesBytes;

                try {
                    if (compact) {
                        if (node.version().compareTo(COMPACT_MAP_ENTRIES_SINCE) < 0) {
                            Collection<Map.Entry<K, V>> entries0 = new ArrayList<>(entries.size());

                            GridPortableProcessor portable = ctx.portable();

                            for (Map.Entry<K, V> entry : entries)
                                entries0.add(new Entry0<>(
                                    portableEnabled ? (K)portable.marshalToPortable(entry.getKey()) : entry.getKey(),
                                    portableEnabled ? (V)portable.marshalToPortable(entry.getValue()) : entry.getValue()));

                            entriesBytes = ctx.config().getMarshaller().marshal(entries0);
                        }
                        else
                            entriesBytes = ctx.config().getMarshaller()
                                .marshal(new Entries0<>(entries, portableEnabled ? ctx.portable() : null));
                    }
                    else
                        entriesBytes = ctx.config().getMarshaller().marshal(entries);

                    if (updaterBytes == null) {
                        assert updater != null;

                        updaterBytes = ctx.config().getMarshaller().marshal(updater);
                    }

                    if (topicBytes == null)
                        topicBytes = ctx.config().getMarshaller().marshal(topic);
                }
                catch (GridException e) {
                    U.error(log, "Failed to marshal (request will not be sent).", e);

                    return;
                }

                GridDeployment dep = null;
                GridPeerDeployAware jobPda0 = null;

                if (ctx.deploy().enabled()) {
                    try {
                        jobPda0 = jobPda;

                        assert jobPda0 != null;

                        dep = ctx.deploy().deploy(jobPda0.deployClass(), jobPda0.classLoader());

                        GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

                        if (cache != null)
                            cache.context().deploy().onEnter();
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to deploy class (request will not be sent): " + jobPda0.deployClass(), e);

                        return;
                    }

                    if (dep == null)
                        U.warn(log, "Failed to deploy class (request will be sent): " + jobPda0.deployClass());
                }

                long reqId = idGen.incrementAndGet();

                fut = curFut;

                reqs.put(reqId, (GridFutureAdapter<Object>)fut);

                GridDataLoadRequest<Object, Object> req = new GridDataLoadRequest<>(
                    reqId,
                    topicBytes,
                    cacheName,
                    updaterBytes,
                    entriesBytes,
                    true,
                    dep != null ? dep.deployMode() : null,
                    dep != null ? jobPda0.deployClass().getName() : null,
                    dep != null ? dep.userVersion() : null,
                    dep != null ? dep.participants() : null,
                    dep != null ? dep.classLoaderId() : null,
                    dep == null);

                try {
                    ctx.io().send(node, TOPIC_DATALOAD, req, PUBLIC_POOL);

                    if (log.isDebugEnabled())
                        log.debug("Sent request to node [nodeId=" + node.id() + ", req=" + req + ']');
                }
                catch (GridException e) {
                    if (ctx.discovery().alive(node) && ctx.discovery().pingNode(node.id()))
                        ((GridFutureAdapter<Object>)fut).onDone(e);
                    else
                        ((GridFutureAdapter<Object>)fut).onDone(new GridTopologyException("Failed to send " +
                            "request (node has left): " + node.id()));
                }
            }
        }

        /**
         *
         */
        void onNodeLeft() {
            assert !isLocNode;
            assert bufMappings.get(node.id()) != this;

            if (log.isDebugEnabled())
                log.debug("Forcibly completing futures (node has left): " + node.id());

            Exception e = new GridTopologyException("Failed to wait for request completion " +
                "(node has left): " + node.id());

            for (GridFutureAdapter<Object> f : reqs.values())
                f.onDone(e);

            // Make sure to complete current future.
            GridFutureAdapter<Object> curFut0;

            synchronized (this) {
                curFut0 = curFut;
            }

            curFut0.onDone(e);
        }

        /**
         * @param res Response.
         */
        void onResponse(GridDataLoadResponse res) {
            if (log.isDebugEnabled())
                log.debug("Received data load response: " + res);

            GridFutureAdapter<?> f = reqs.remove(res.requestId());

            if (f == null) {
                if (log.isDebugEnabled())
                    log.debug("Future for request has not been found: " + res.requestId());

                return;
            }

            Throwable err = null;

            byte[] errBytes = res.errorBytes();

            if (errBytes != null) {
                try {
                    GridPeerDeployAware jobPda0 = jobPda;

                    err = ctx.config().getMarshaller().unmarshal(
                        errBytes,
                        jobPda0 != null ? jobPda0.classLoader() : U.gridClassLoader());
                }
                catch (GridException e) {
                    f.onDone(null, new GridException("Failed to unmarshal response.", e));

                    return;
                }
            }

            f.onDone(null, err);

            if (log.isDebugEnabled())
                log.debug("Finished future [fut=" + f + ", reqId=" + res.requestId() + ", err=" + err + ']');
        }

        /**
         *
         */
        void cancelAll() {
            GridException err = new GridException("Data loader has been cancelled: " + IgniteDataLoaderImpl.this);

            for (IgniteFuture<?> f : locFuts) {
                try {
                    f.cancel();
                }
                catch (GridException e) {
                    U.error(log, "Failed to cancel mini-future.", e);
                }
            }

            for (GridFutureAdapter<?> f : reqs.values())
                f.onDone(err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            int size;

            synchronized (this) {
                size = entries.size();
            }

            return S.toString(Buffer.class, this,
                "entriesCnt", size,
                "locFutsSize", locFuts.size(),
                "reqsSize", reqs.size());
        }
    }

    /**
     * Data loader peer-deploy aware.
     */
    private class DataLoaderPda implements GridPeerDeployAware {
        /** */
        private static final long serialVersionUID = 0L;

        /** Deploy class. */
        private Class<?> cls;

        /** Class loader. */
        private ClassLoader ldr;

        /** Collection of objects to detect deploy class and class loader. */
        private Collection<Object> objs;

        /**
         * Constructs data loader peer-deploy aware.
         *
         * @param objs Collection of objects to detect deploy class and class loader.
         */
        private DataLoaderPda(Object... objs) {
            this.objs = Arrays.asList(objs);
        }

        /** {@inheritDoc} */
        @Override public Class<?> deployClass() {
            if (cls == null) {
                Class<?> cls0 = null;

                if (depCls != null)
                    cls0 = depCls;
                else {
                    for (Iterator<Object> it = objs.iterator(); (cls0 == null || U.isJdk(cls0)) && it.hasNext();) {
                        Object o = it.next();

                        if (o != null)
                            cls0 = U.detectClass(o);
                    }

                    if (cls0 == null || U.isJdk(cls0))
                        cls0 = IgniteDataLoaderImpl.class;
                }

                assert cls0 != null : "Failed to detect deploy class [objs=" + objs + ']';

                cls = cls0;
            }

            return cls;
        }

        /** {@inheritDoc} */
        @Override public ClassLoader classLoader() {
            if (ldr == null) {
                ClassLoader ldr0 = deployClass().getClassLoader();

                // Safety.
                if (ldr0 == null)
                    ldr0 = U.gridClassLoader();

                assert ldr0 != null : "Failed to detect classloader [objs=" + objs + ']';

                ldr = ldr0;
            }

            return ldr;
        }
    }

    /**
     * Entry.
     */
    private static class Entry0<K, V> implements Map.Entry<K, V>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private K key;

        /** */
        private V val;

        /**
         * @param key Key.
         * @param val Value.
         */
        private Entry0(K key, @Nullable V val) {
            assert key != null;

            this.key = key;
            this.val = val;
        }

        /**
         * For {@link Externalizable}.
         */
        @SuppressWarnings("UnusedDeclaration")
        public Entry0() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public V setValue(V val) {
            V old = this.val;

            this.val = val;

            return old;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(key);
            out.writeObject(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = (K)in.readObject();
            val = (V)in.readObject();
        }
    }

    /**
     * Wrapper list with special compact serialization of map entries.
     */
    private static class Entries0<K, V> extends AbstractCollection<Map.Entry<K, V>> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /**  Wrapped delegate. */
        private Collection<Map.Entry<K, V>> delegate;

        /** Optional portable processor for converting values. */
        private GridPortableProcessor portable;

        /**
         * @param delegate Delegate.
         * @param portable Portable processor.
         */
        private Entries0(Collection<Map.Entry<K, V>> delegate, GridPortableProcessor portable) {
            this.delegate = delegate;
            this.portable = portable;
        }

        /**
         * For {@link Externalizable}.
         */
        public Entries0() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Entry<K, V>> iterator() {
            return delegate.iterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return delegate.size();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(delegate.size());

            boolean portableEnabled = portable != null;

            for (Map.Entry<K, V> entry : delegate) {
                if (portableEnabled) {
                    out.writeObject(portable.marshalToPortable(entry.getKey()));
                    out.writeObject(portable.marshalToPortable(entry.getValue()));
                }
                else {
                    out.writeObject(entry.getKey());
                    out.writeObject(entry.getValue());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int sz = in.readInt();

            delegate = new ArrayList<>(sz);

            for (int i = 0; i < sz; i++) {
                Object k = in.readObject();
                Object v = in.readObject();

                delegate.add(new Entry0<>((K)k, (V)v));
            }
        }
    }
}
