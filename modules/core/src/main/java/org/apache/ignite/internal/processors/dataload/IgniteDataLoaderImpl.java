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

package org.apache.ignite.internal.processors.dataload;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.dataload.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.portable.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.internal.GridNodeAttributes.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;

/**
 * Data loader implementation.
 */
public class IgniteDataLoaderImpl<K, V> implements IgniteDataLoader<K, V>, Delayed {
    /** */
    public static final IgniteProductVersion COMPACT_MAP_ENTRIES_SINCE = IgniteProductVersion.fromString("1.0.0");

    /** Cache updater. */
    private IgniteDataLoadCacheUpdater<K, V> updater = GridDataLoadCacheUpdaters.individual();

    /** */
    private byte[] updaterBytes;

    /** Max remap count before issuing an error. */
    private static final int MAX_REMAP_CNT = 32;

    /** Log reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

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
    private IgniteLogger log;

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
    private final Collection<IgniteInternalFuture<?>> activeFuts = new GridConcurrentHashSet<>();

    /** Closure to remove from active futures. */
    @GridToStringExclude
    private final IgniteInClosure<IgniteInternalFuture<?>> rmvActiveFut = new IgniteInClosure<IgniteInternalFuture<?>>() {
        @Override public void apply(IgniteInternalFuture<?> t) {
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

    /** Public API future to track loading finish. */
    private final IgniteFuture<?> publicFut;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private volatile long lastFlushTime = U.currentTimeMillis();

    /** */
    private final DelayQueue<IgniteDataLoaderImpl<K, V>> flushQ;

    /** */
    private boolean skipStore;

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

        publicFut = new IgniteFutureImpl<>(fut);
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
        return publicFut;
    }

    /**
     * @return Internal future.
     */
    public IgniteInternalFuture<?> internalFuture() {
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
    @Override public void isolated(boolean isolated) {
        if (isolated())
            return;

        ClusterNode node = F.first(ctx.grid().forCache(cacheName).nodes());

        if (node == null)
            throw new IgniteException("Failed to get node for cache: " + cacheName);

        GridCacheAttributes a = U.cacheAttributes(node, cacheName);

        assert a != null;

        updater = a.atomicityMode() == CacheAtomicityMode.ATOMIC ?
            GridDataLoadCacheUpdaters.<K, V>batched() :
            GridDataLoadCacheUpdaters.<K, V>groupLocked();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public void skipStore(boolean skipStore) {
        this.skipStore = skipStore;
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

            resFut.listenAsync(rmvActiveFut);

            activeFuts.add(resFut);

            Collection<K> keys = null;

            if (entries.size() > 1) {
                keys = new GridConcurrentHashSet<>(entries.size(), U.capacity(entries.size()), 1);

                for (Map.Entry<K, V> entry : entries)
                    keys.add(entry.getKey());
            }

            load0(entries, resFut, keys, 0);

            return new IgniteFutureImpl<>(resFut);
        }
        catch (IgniteException e) {
            return new IgniteFinishedFutureImpl<>(ctx, e);
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Map.Entry<K, V> entry) {
        A.notNull(entry, "entry");

        return addData(F.asList(entry));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(K key, V val) {
        A.notNull(key, "key");

        return addData(new Entry0<>(key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeData(K key) {
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
        @Nullable final Collection<K> activeKeys,
        final int remaps
    ) {
        assert entries != null;

        if (remaps >= MAX_REMAP_CNT) {
            resFut.onDone(new IgniteCheckedException("Failed to finish operation (too many remaps): " + remaps));

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
            catch (IgniteCheckedException e) {
                resFut.onDone(e);

                return;
            }

            if (node == null) {
                resFut.onDone(new ClusterTopologyCheckedException("Failed to map key to node " +
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

            IgniteInClosure<IgniteInternalFuture<?>> lsnr = new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> t) {
                    try {
                        t.get();

                        if (activeKeys != null) {
                            for (Map.Entry<K, V> e : entriesForNode)
                                activeKeys.remove(e.getKey());

                            if (activeKeys.isEmpty())
                                resFut.onDone();
                        }
                        else {
                            assert entriesForNode.size() == 1;

                            // That has been a single key,
                            // so complete result future right away.
                            resFut.onDone();
                        }
                    }
                    catch (IgniteCheckedException e1) {
                        if (log.isDebugEnabled())
                            log.debug("Future finished with error [nodeId=" + nodeId + ", err=" + e1 + ']');

                        if (cancelled) {
                            resFut.onDone(new IgniteCheckedException("Data loader has been cancelled: " +
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
            catch (IgniteInterruptedCheckedException e1) {
                resFut.onDone(e1);

                return;
            }

            if (ctx.discovery().node(nodeId) == null) {
                if (bufMappings.remove(nodeId, buf))
                    buf.onNodeLeft();

                if (f != null)
                    f.onDone(new ClusterTopologyCheckedException("Failed to wait for request completion " +
                        "(node has left): " + nodeId));
            }
        }
    }

    /**
     * Performs flush.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void doFlush() throws IgniteCheckedException {
        lastFlushTime = U.currentTimeMillis();

        List<IgniteInternalFuture> activeFuts0 = null;

        int doneCnt = 0;

        for (IgniteInternalFuture<?> f : activeFuts) {
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
            Queue<IgniteInternalFuture<?>> q = null;

            for (Buffer buf : bufMappings.values()) {
                IgniteInternalFuture<?> flushFut = buf.flush();

                if (flushFut != null) {
                    if (q == null)
                        q = new ArrayDeque<>(bufMappings.size() * 2);

                    q.add(flushFut);
                }
            }

            if (q != null) {
                assert !q.isEmpty();

                boolean err = false;

                for (IgniteInternalFuture fut = q.poll(); fut != null; fut = q.poll()) {
                    try {
                        fut.get();
                    }
                    catch (IgniteCheckedException e) {
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
                IgniteInternalFuture f = activeFuts0.get(i);

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
    @Override public void flush() throws IgniteException {
        enterBusy();

        try {
            doFlush();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
    @Override public void tryFlush() throws IgniteInterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            for (Buffer buf : bufMappings.values())
                buf.flush();

            lastFlushTime = U.currentTimeMillis();
        }
        catch (IgniteInterruptedCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @throws IgniteException If failed.
     */
    @Override public void close(boolean cancel) throws IgniteException {
        try {
            closeEx(cancel);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @throws IgniteCheckedException If failed.
     */
    public void closeEx(boolean cancel) throws IgniteCheckedException {
        if (!closed.compareAndSet(false, true))
            return;

        busyLock.block();

        if (log.isDebugEnabled())
            log.debug("Closing data loader [ldr=" + this + ", cancel=" + cancel + ']');

        IgniteCheckedException e = null;

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
        catch (IgniteCheckedException e0) {
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
    @Override public void close() throws IgniteException {
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
        private final Collection<IgniteInternalFuture<Object>> locFuts;

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
        private final IgniteInClosure<IgniteInternalFuture<Object>> signalC = new IgniteInClosure<IgniteInternalFuture<Object>>() {
            @Override public void apply(IgniteInternalFuture<Object> t) {
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
         * @throws IgniteInterruptedCheckedException If failed.
         * @return Future for operation.
         */
        @Nullable GridFutureAdapter<?> update(Iterable<Map.Entry<K, V>> newEntries,
            IgniteInClosure<IgniteInternalFuture<?>> lsnr) throws IgniteInterruptedCheckedException {
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
                    curFut0.onDone(new IgniteCheckedException("Data loader has been cancelled: " + IgniteDataLoaderImpl.this));
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
         * @throws IgniteInterruptedCheckedException If thread has been interrupted.
         */
        @Nullable IgniteInternalFuture<?> flush() throws IgniteInterruptedCheckedException {
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

            for (IgniteInternalFuture<Object> f : locFuts) {
                if (res == null)
                    res = new GridCompoundFuture<>(ctx);

                res.add(f);
            }

            for (IgniteInternalFuture<Object> f : reqs.values()) {
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
         * @throws IgniteInterruptedCheckedException If thread has been interrupted.
         */
        private void incrementActiveTasks() throws IgniteInterruptedCheckedException {
            U.acquire(sem);
        }

        /**
         * @param f Future that finished.
         */
        private void signalTaskFinished(IgniteInternalFuture<Object> f) {
            assert f != null;

            sem.release();
        }

        /**
         * @param entries Entries to submit.
         * @param curFut Current future.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private void submit(final Collection<Map.Entry<K, V>> entries, final GridFutureAdapter<Object> curFut)
            throws IgniteInterruptedCheckedException {
            assert entries != null;
            assert !entries.isEmpty();
            assert curFut != null;

            incrementActiveTasks();

            IgniteInternalFuture<Object> fut;

            if (isLocNode) {
                fut = ctx.closure().callLocalSafe(
                    new GridDataLoadUpdateJob<>(ctx, log, cacheName, entries, false, skipStore, updater), false);

                locFuts.add(fut);

                fut.listenAsync(new IgniteInClosure<IgniteInternalFuture<Object>>() {
                    @Override public void apply(IgniteInternalFuture<Object> t) {
                        try {
                            boolean rmv = locFuts.remove(t);

                            assert rmv;

                            curFut.onDone(t.get());
                        }
                        catch (IgniteCheckedException e) {
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
                catch (IgniteCheckedException e) {
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
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to deploy class (request will not be sent): " + jobPda0.deployClass(), e);

                        return;
                    }

                    if (dep == null)
                        U.warn(log, "Failed to deploy class (request will be sent): " + jobPda0.deployClass());
                }

                long reqId = idGen.incrementAndGet();

                fut = curFut;

                reqs.put(reqId, (GridFutureAdapter<Object>)fut);

                GridDataLoadRequest req = new GridDataLoadRequest(
                    reqId,
                    topicBytes,
                    cacheName,
                    updaterBytes,
                    entriesBytes,
                    true,
                    skipStore,
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
                catch (IgniteCheckedException e) {
                    if (ctx.discovery().alive(node) && ctx.discovery().pingNode(node.id()))
                        ((GridFutureAdapter<Object>)fut).onDone(e);
                    else
                        ((GridFutureAdapter<Object>)fut).onDone(new ClusterTopologyCheckedException("Failed to send " +
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

            Exception e = new ClusterTopologyCheckedException("Failed to wait for request completion " +
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
                catch (IgniteCheckedException e) {
                    f.onDone(null, new IgniteCheckedException("Failed to unmarshal response.", e));

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
            IgniteCheckedException err = new IgniteCheckedException("Data loader has been cancelled: " + IgniteDataLoaderImpl.this);

            for (IgniteInternalFuture<?> f : locFuts) {
                try {
                    f.cancel();
                }
                catch (IgniteCheckedException e) {
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
