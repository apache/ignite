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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.systemview.walker.MetastorageViewWalker;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageListener;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.apache.ignite.spi.systemview.view.MetastorageView;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static java.util.function.Function.identity;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.META_STORAGE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;
import static org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage.isSupported;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemPrefix;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemVer;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.marshal;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageVersion.INITIAL_VERSION;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * <p>Implementation of {@link DistributedMetaStorage} based on {@link MetaStorage} for persistence and discovery SPI
 * for communication.
 * </p>
 * <p>It is based on existing local metastorage API for persistent clusters (in-memory clusters and client nodes will
 * store data in memory). Write/remove operation use Discovery SPI to send updates to the cluster, it guarantees updates
 * order and the fact that all existing (alive) nodes have handled the update message.
 * </p>
 * <p>As a way to find out which node has the latest data there is a "version" value of distributed metastorage,
 * ({@link DistributedMetaStorageVersion}) which is basically the pair {@code <number of all updates, hash of all
 * updates>}. First element of the pair is the value of {@link #getUpdatesCount()}.
 * </p>
 * <p>Whole updates history until some point in the past is stored along with the data, so when an outdated node
 * connects to the cluster it will receive all the missing data and apply it locally. Listeners will also be invoked
 * after such updates. If there's not enough history stored or joining node is clear then it'll receive shapshot of
 * distributed metastorage (usually called {@code fullData} in code) so there won't be inconsistencies.
 * </p>
 *
 * @see DistributedMetaStorageUpdateMessage
 * @see DistributedMetaStorageUpdateAckMessage
 */
public class DistributedMetaStorageImpl extends GridProcessorAdapter
    implements DistributedMetaStorage, IgniteChangeGlobalStateSupport {
    /** Component ID required for {@link DiscoveryDataBag} instances. */
    private static final int COMPONENT_ID = META_STORAGE.ordinal();

    /** Default upper bound of history size in bytes. */
    private static final long DFLT_MAX_HISTORY_BYTES = 100 * 1024 * 1024;

    /** Message indicating that clusted is in a mixed state and writing cannot be completed because of that. */
    public static final String NOT_SUPPORTED_MSG = "Ignite cluster has nodes that don't support" +
        " distributed metastorage feature. Writing cannot be completed.";

    /** Name of the system view for a system {@link MetaStorage}. */
    public static final String DISTRIBUTED_METASTORE_VIEW = metricName("distributed", "metastorage");

    /** Description of the system view for a {@link MetaStorage}. */
    public static final String DISTRIBUTED_METASTORE_VIEW_DESC = "Distributed metastorage data";

    /**
     * {@code true} if local node is client.
     */
    private final boolean isClient;

    /**
     * {@code true} if local node has persistent region in configuration and is not a client.
     */
    private final boolean isPersistenceEnabled;

    /**
     * Cached subscription processor instance. Exists to make code shorter.
     */
    private final GridInternalSubscriptionProcessor isp;

    /** */
    private volatile InMemoryCachedDistributedMetaStorageBridge bridge;

    /**
     * Version of distributed metastorage.
     */
    private volatile DistributedMetaStorageVersion ver;

    /**
     * Listeners collection. Preserves the order in which listeners were added.
     */
    final List<IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>>> lsnrs =
        new CopyOnWriteArrayList<>();

    /**
     * All available history. Contains latest changes in distributed metastorage. Latest version is always present in
     * the cache. This means that the it is empty only if version is {@code 0}.
     */
    private final DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

    /**
     * Maximal acceptable value of {@link #histCache}'s size in bytes. History will shrink after every write until its
     * size is not greater then given value.
     */
    private final long histMaxBytes = IgniteSystemProperties.getLong(
        IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES,
        DFLT_MAX_HISTORY_BYTES
    );

    /**
     * Map with futures used to wait for async write/remove operations completion.
     */
    private final ConcurrentMap<UUID, GridFutureAdapter<Boolean>> updateFuts = new ConcurrentHashMap<>();

    /**
     * Lock to access/update data and component's state.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Context marshaller.
     */
    private final JdkMarshaller marshaller;

    /**
     * Worker that will write data on disk asynchronously. Makes sence for persistent nodes only.
     */
    private final DmsDataWriterWorker worker;

    /**
     * @param ctx Kernal context.
     */
    public DistributedMetaStorageImpl(GridKernalContext ctx) {
        super(ctx);

        isClient = ctx.clientNode();

        isPersistenceEnabled = !isClient && isPersistenceEnabled(ctx.config());

        isp = ctx.internalSubscriptionProcessor();

        marshaller = ctx.marshallerContext().jdkMarshaller();

        bridge = new InMemoryCachedDistributedMetaStorageBridge(marshaller);

        //noinspection IfMayBeConditional
        if (!isPersistenceEnabled)
            worker = null;
        else {
            worker = new DmsDataWriterWorker(
                ctx.igniteInstanceName(),
                log,
                new DmsLocalMetaStorageLock() {
                    /** {@inheritDoc} */
                    @Override public void lock() {
                        localMetastorageLock();
                    }

                    /** {@inheritDoc} */
                    @Override public void unlock() {
                        localMetastorageUnlock();
                    }
                },
                this::criticalError
            );
        }

        ctx.discovery().localJoinFuture().listen(this::notifyReadyForWrite);
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Create all required listeners.
     */
    @Override public void start() throws IgniteCheckedException {
        if (!isPersistenceEnabled)
            ver = INITIAL_VERSION;
        else {
            isp.registerMetastorageListener(new MetastorageLifecycleListener() {
                /** {@inheritDoc} */
                @Override public void onReadyForRead(
                    ReadOnlyMetastorage metastorage
                ) throws IgniteCheckedException {
                    onMetaStorageReadyForRead(metastorage);
                }

                /** {@inheritDoc} */
                @Override public void onReadyForReadWrite(
                    ReadWriteMetastorage metastorage
                ) {
                    onMetaStorageReadyForWrite(metastorage);
                }
            });
        }

        GridDiscoveryManager discovery = ctx.discovery();

        discovery.setCustomEventListener(
            DistributedMetaStorageUpdateMessage.class,
            this::onUpdateMessage
        );

        discovery.setCustomEventListener(
            DistributedMetaStorageUpdateAckMessage.class,
            this::onAckMessage
        );
    }

    /**
     * {@inheritDoc}
     * <br/>
     * For persistent cluster it will stop the async worker.
     *
     * @param cancel If {@code true} then ignore worker's queue and finish it as fast as possible. Otherwise
     *      just wait until queue is empty and worker completed its job.
     */
    @Override public void onKernalStop(boolean cancel) {
        lock.writeLock().lock();

        try {
            stopWorker(cancel);
        }
        finally {
            lock.writeLock().unlock();

            cancelUpdateFutures();
        }
    }

    /** */
    private void stopWorker(boolean cancel) {
        assert lock.isWriteLockedByCurrentThread();

        if (isPersistenceEnabled) {
            try {
                worker.cancel(cancel);
            }
            catch (InterruptedException e) {
                log.error("Cannot stop distributed metastorage worker.", e);
            }
        }
    }

    /**
     * Executed roughly at the same time as {@link #onMetaStorageReadyForRead(ReadOnlyMetastorage)}.
     */
    public void inMemoryReadyForRead() {
        if (!isPersistenceEnabled) {
            registerSystemView();

            notifyReadyForRead();
        }
    }

    /** */
    private void registerSystemView() {
        ctx.systemView().registerView(DISTRIBUTED_METASTORE_VIEW, DISTRIBUTED_METASTORE_VIEW_DESC,
            new MetastorageViewWalker(), () -> {
                try {
                    List<MetastorageView> data = new ArrayList<>();

                    iterate("", (key, val) -> data.add(new MetastorageView(key, IgniteUtils.toStringSafe(val))));

                    return data;
                }
                catch (IgniteCheckedException e) {
                    log.warning("Metastore iteration error", e);

                    return Collections.emptyList();
                }
            }, identity());

    }

    /** Notify components listeners. */
    private void notifyReadyForRead() {
        for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
            subscriber.onReadyForRead(this);
    }

    /**
     * Notify components listeners.
     *
     * @param fut Local join future. There won't be any notifications for that failed it's join.
     */
    private void notifyReadyForWrite(IgniteInternalFuture<DiscoveryLocalJoinData> fut) {
        if (fut.error() == null)
            for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
                subscriber.onReadyForWrite(this);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {
    }

    /**
     * {@inheritDoc}
     * <br/>
     * For persistent nodes wait until worker's queue is empty and worker completed its job.
     */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (isClient)
            return;

        lock.writeLock().lock();

        try {
            stopWorker(false);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Whether cluster is active at this moment or not. Also returns {@code true} if cluster is being activated.
     */
    private boolean isActive() {
        return ctx.state().clusterState().active();
    }

    /**
     * Implementation for {@link MetastorageLifecycleListener#onReadyForRead(ReadOnlyMetastorage)} listener. Invoked
     * after node was started but before it was activated (only in persistent clusters).
     *
     * @param metastorage Local metastorage instance available for reading.
     * @throws IgniteCheckedException If there were any issues while metastorage reading.
     * @see MetastorageLifecycleListener#onReadyForRead(ReadOnlyMetastorage)
     */
    private void onMetaStorageReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        assert isPersistenceEnabled;

        localMetastorageLock();

        try {
            lock.writeLock().lock();

            try {
                ver = bridge.readInitialData(metastorage);

                metastorage.iterate(
                    historyItemPrefix(),
                    (key, val) -> addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                    true
                );
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            localMetastorageUnlock();
        }

        registerSystemView();

        notifyReadyForRead();
    }

    /**
     * Implementation for {@link MetastorageLifecycleListener#onReadyForReadWrite(ReadWriteMetastorage)} listener.
     * Invoked after each activation (only in persistent clusters). Restarts async worker.
     *
     * @param metastorage Local metastorage instance available for writing.
     */
    private void onMetaStorageReadyForWrite(ReadWriteMetastorage metastorage) {
        assert isPersistenceEnabled;

        worker.setMetaStorage(metastorage);

        IgniteThread workerThread = new IgniteThread(ctx.igniteInstanceName(), "dms-writer-thread", worker);

        workerThread.start();
    }

    /** {@inheritDoc} */
    @Override public long getUpdatesCount() {
        return ver.id();
    }

    /** {@inheritDoc} */
    @Override @Nullable public <T extends Serializable> T read(@NotNull String key) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            return (T)bridge.read(key);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException {
        assert val != null : key;

        startWrite(key, marshal(marshaller, val)).get();
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> writeAsync(
        @NotNull String key,
        @NotNull Serializable val
    ) throws IgniteCheckedException {
        assert val != null : key;

        return startWrite(key, marshal(marshaller, val));
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> removeAsync(@NotNull String key) throws IgniteCheckedException {
        return startWrite(key, null);
    }

    /** {@inheritDoc} */
    @Override public void remove(@NotNull String key) throws IgniteCheckedException {
        startWrite(key, null).get();
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException {
        assert newVal != null : key;

        return compareAndSetAsync(key, expVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<Boolean> compareAndSetAsync(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException {
        assert newVal != null : key;

        return startCas(key, marshal(marshaller, expVal), marshal(marshaller, newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndRemove(
        @NotNull String key,
        @NotNull Serializable expVal
    ) throws IgniteCheckedException {
        assert expVal != null : key;

        return startCas(key, marshal(marshaller, expVal), null).get();
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        @NotNull String keyPrefix,
        @NotNull BiConsumer<String, ? super Serializable> cb
    ) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            bridge.iterate(keyPrefix, cb);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void listen(@NotNull Predicate<String> keyPred, DistributedMetaStorageListener<?> lsnr) {
        DistributedMetaStorageListener<Serializable> lsnrUnchecked = (DistributedMetaStorageListener<Serializable>)lsnr;

        lsnrs.add(new IgniteBiTuple<>(keyPred, lsnrUnchecked));
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryDataExchangeType discoveryDataType() {
        return META_STORAGE;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        lock.readLock().lock();

        try {
            if (isClient) {
                Serializable data = new DistributedMetaStorageJoiningNodeData(
                    getBaselineTopologyId(),
                    ver,
                    EMPTY_ARRAY
                );

                try {
                    dataBag.addJoiningNodeData(COMPONENT_ID, marshaller.marshal(data));

                    return;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            Serializable data = new DistributedMetaStorageJoiningNodeData(
                getBaselineTopologyId(),
                ver,
                histCache.toArray()
            );

            try {
                dataBag.addJoiningNodeData(COMPONENT_ID, marshaller.marshal(data));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return Current baseline topology id or {@code -1} if there was no baseline topology found.
     */
    private int getBaselineTopologyId() {
        BaselineTopology baselineTop = ctx.state().clusterState().baselineTopology();

        return baselineTop != null ? baselineTop.id() : -1;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * If local node is client then method should do nothing. It is expected that this method is invoked on coordinator
     * node, but there might be exceptions to this. Validation rules:
     * <ul>
     *     <li>
     *         Do not join node that has no distributed metastorage if feature is supported in current topology and
     *         distributed metastorage has already been used ({@link #getUpdatesCount()} is not zero).
     *     </li>
     *     <li>
     *         Do not join node that has updates count greater then on local node and hasn't provided enough history
     *         to apply it to the cluster.
     *     </li>
     *     <li>
     *         Do not join node if its distributed metastorage version hash differs from the local one. In such cases
     *         node is probably from different cluster or has some inconsistent data.
     *     </li>
     * </ul>
     */
    @Override @Nullable public IgniteNodeValidationResult validateNode(
        ClusterNode node,
        JoiningNodeDiscoveryData discoData
    ) {
        if (isClient)
            return null;

        lock.readLock().lock();

        try {
            DistributedMetaStorageVersion locVer = ver;

            if (!discoData.hasJoiningNodeData()) {
                // Joining node doesn't support distributed metastorage feature.

                if (isSupported(ctx) && locVer.id() > 0 && !(node.isClient() || node.isDaemon())) {
                    String errorMsg = "Node not supporting distributed metastorage feature" +
                        " is not allowed to join the cluster";

                    return new IgniteNodeValidationResult(node.id(), errorMsg);
                }
                else
                    return null;
            }

            DistributedMetaStorageJoiningNodeData joiningData = getJoiningNodeData(discoData);

            if (joiningData == null) {
                String errorMsg = "Cannot unmarshal joining node data";

                return new IgniteNodeValidationResult(node.id(), errorMsg);
            }

            if (!isPersistenceEnabled)
                return null;

            DistributedMetaStorageVersion remoteVer = joiningData.ver;

            DistributedMetaStorageHistoryItem[] remoteHist = joiningData.hist;

            int remoteHistSize = remoteHist.length;

            int remoteBltId = joiningData.bltId;

            boolean clusterIsActive = isActive();

            String errorMsg;

            int locBltId = getBaselineTopologyId();

            int locHistSize = histCache.size();

            if (remoteVer.id() < locVer.id() - locHistSize) {
                // Remote node is too far behind.
                // Technicaly this situation should be banned because there's no way to prove data consistency.
                errorMsg = null;
            }
            else if (remoteVer.id() < locVer.id()) {
                // Remote node it behind the cluster version and there's enough history.
                DistributedMetaStorageVersion newRemoteVer = remoteVer.nextVersion(
                    this::historyItem,
                    remoteVer.id() + 1,
                    locVer.id()
                );

                if (newRemoteVer.equals(locVer))
                    errorMsg = null;
                else
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
            }
            else if (remoteVer.id() == locVer.id()) {
                // Remote and local versions match.
                if (remoteVer.equals(locVer))
                    errorMsg = null;
                else {
                    errorMsg = S.toString(
                        "Joining node has conflicting distributed metastorage data:",
                        "clusterVersion", locVer, false,
                        "joiningNodeVersion", remoteVer, false
                    );
                }
            }
            else if (remoteVer.id() <= locVer.id() + remoteHistSize) {
                // Remote node is ahead of the cluster and has enough history.
                if (clusterIsActive) {
                    errorMsg = "Attempting to join node with larger distributed metastorage version id." +
                        " The node is most likely in invalid state and can't be joined.";
                }
                else if (remoteBltId < locBltId)
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
                else {
                    DistributedMetaStorageVersion newLocVer = locVer.nextVersion(
                        remoteHist,
                        remoteHistSize - (int)(remoteVer.id() - locVer.id()),
                        remoteHistSize
                    );

                    if (newLocVer.equals(remoteVer))
                        errorMsg = null;
                    else
                        errorMsg = "Joining node has conflicting distributed metastorage data.";
                }
            }
            else {
                assert remoteVer.id() > locVer.id() + remoteHistSize;

                // Remote node is too far ahead.
                if (clusterIsActive) {
                    errorMsg = "Attempting to join node with larger distributed metastorage version id." +
                        " The node is most likely in invalid state and can't be joined.";
                }
                else if (remoteBltId < locBltId)
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
                else {
                    errorMsg = "Joining node doesn't have enough history items in distributed metastorage data." +
                        " Please check the order in which you start cluster nodes.";
                }
            }

            if (errorMsg == null)
                errorMsg = validatePayload(joiningData);

            return (errorMsg == null) ? null : new IgniteNodeValidationResult(node.id(), errorMsg);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param joiningData Joining data to validate.
     * @return {@code null} if contained data is valid otherwise error message.
     */
    private String validatePayload(DistributedMetaStorageJoiningNodeData joiningData) {
        for (DistributedMetaStorageHistoryItem item : joiningData.hist) {
            for (int i = 0; i < item.keys().length; i++) {
                try {
                    unmarshal(marshaller, item.valuesBytesArray()[i]);
                }
                catch (IgniteCheckedException e) {
                    return "Unable to unmarshal key=" + item.keys()[i];
                }
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Since {@link #validateNode(ClusterNode, DiscoveryDataBag.JoiningNodeDiscoveryData)} has already been invoked we
     * can be sure that joining node has valid discovery data. Current method does something meaningful only if joining
     * node has bigger distributed metastorage version, in this case all required updates will be applied.
     */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData discoData) {
        if (!discoData.hasJoiningNodeData())
            return;

        DistributedMetaStorageJoiningNodeData joiningData = getJoiningNodeData(discoData);

        if (joiningData == null)
            return;

        DistributedMetaStorageVersion remoteVer = joiningData.ver;

        if (!isSupported(ctx) && remoteVer.id() > 0)
            return;

        lock.writeLock().lock();

        try {
            DistributedMetaStorageVersion locVer = ver;

            if (remoteVer.id() > locVer.id()) {
                DistributedMetaStorageHistoryItem[] hist = joiningData.hist;

                if (remoteVer.id() - locVer.id() <= hist.length) {
                    for (long v = locVer.id() + 1; v <= remoteVer.id(); v++) {
                        int hv = (int)(v - remoteVer.id() + hist.length - 1);

                        try {
                            completeWrite(hist[hv]);
                        }
                        catch (IgniteCheckedException ex) {
                            log.error("Unable to unmarshal new metastore data. update=" + hist[hv], ex);
                        }
                    }
                }
                else
                    assert false : "Joining node is too far ahead [remoteVer=" + remoteVer + "]";
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Does nothing on client nodes. Also does nothinf if feature is not supported on some node in topology.
     * Otherwise it fills databag with data required for joining node so it could be consistent with the cluster.
     * There are 2 main cases: local node has enough history to send only updates or it doesn't. In first case
     * the history is collected, otherwise whole distributed metastorage ({@code fullData}) is collected along with
     * available history. Goal of collecting history in second case is to allow all nodes in cluster to have the same
     * history so connection of new server will always give the same result.
     */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (isClient)
            return;

        if (dataBag.commonDataCollectedFor(COMPONENT_ID))
            return;

        JoiningNodeDiscoveryData discoData = dataBag.newJoinerDiscoveryData(COMPONENT_ID);

        if (!discoData.hasJoiningNodeData())
            return;

        if (!isSupported(ctx))
            return;

        DistributedMetaStorageJoiningNodeData joiningData = getJoiningNodeData(discoData);

        if (joiningData == null)
            return;

        DistributedMetaStorageVersion remoteVer = joiningData.ver;

        lock.readLock().lock();

        try {
            DistributedMetaStorageVersion locVer = ver;

            if (remoteVer.id() >= locVer.id()) {
                Serializable nodeData = new DistributedMetaStorageClusterNodeData(remoteVer, null, null, null);

                dataBag.addGridCommonData(COMPONENT_ID, nodeData);
            }
            else {
                if (locVer.id() - remoteVer.id() <= histCache.size() && !dataBag.isJoiningNodeClient()) {
                    DistributedMetaStorageHistoryItem[] updates = history(remoteVer.id() + 1, locVer.id());

                    Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver, null, null, updates);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
                else {
                    DistributedMetaStorageVersion ver0 = ver;

                    DistributedMetaStorageKeyValuePair[] fullData = bridge.localFullData();

                    DistributedMetaStorageHistoryItem[] hist;

                    if (dataBag.isJoiningNodeClient())
                        hist = EMPTY_ARRAY;
                    else
                        hist = history(ver.id() - histCache.size() + 1, locVer.id());

                    Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver0, fullData, hist, null);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieve joining node data from discovery data. It is expected that it is present as a {@code byte[]} object.
     *
     * @param discoData Joining node discovery data.
     * @return Unmarshalled data or null if unmarshalling failed.
     */
    @Nullable private DistributedMetaStorageJoiningNodeData getJoiningNodeData(
        JoiningNodeDiscoveryData discoData
    ) {
        byte[] data = (byte[])discoData.joiningNodeData();

        assert data != null;

        try {
            return marshaller.unmarshal(data, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to unmarshal joinging node data for distributed metastorage component.", e);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        assert isClient;

        lock.writeLock().lock();

        try {
            bridge = new InMemoryCachedDistributedMetaStorageBridge(marshaller);

            ver = INITIAL_VERSION;

            cancelUpdateFutures();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cancel all waiting futures and clear the map.
     */
    private void cancelUpdateFutures() {
        for (GridFutureAdapter<Boolean> fut : updateFuts.values())
            fut.onDone(new IgniteCheckedException("Client was disconnected during the operation."));

        updateFuts.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) {
        assert isClient;

        ctx.discovery().localJoinFuture().listen(this::notifyReadyForWrite);

        return null;
    }

    /**
     * Returns last update for the specified version.
     *
     * @param specificVer Specific version.
     * @return {@code <key, value>} pair if it was found, {@code null} otherwise.
     */
    private DistributedMetaStorageHistoryItem historyItem(long specificVer) {
        return histCache.get(specificVer);
    }

    /**
     * Returns all updates in the specified range of versions.
     *
     * @param startVer Lower bound (inclusive).
     * @param actualVer Upper bound (inclusive).
     * @return Array with all requested updates sorted by version in ascending order.
     */
    private DistributedMetaStorageHistoryItem[] history(long startVer, long actualVer) {
        return LongStream.rangeClosed(startVer, actualVer)
            .mapToObj(this::historyItem)
            .toArray(DistributedMetaStorageHistoryItem[]::new);
    }

    /**
     * {@link InMemoryCachedDistributedMetaStorageBridge#localFullData()} invoked on {@link #bridge}.
     */
    @TestOnly
    private DistributedMetaStorageKeyValuePair[] localFullData() {
        return bridge.localFullData();
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Applies received updates if they are present in response.
     *
     * @param data Grid discovery data.
     */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        lock.writeLock().lock();

        try {
            DistributedMetaStorageClusterNodeData nodeData = (DistributedMetaStorageClusterNodeData)data.commonData();

            if (nodeData != null) {
                if (nodeData.fullData != null) {
                    ver = nodeData.ver;

                    notifyListenersBeforeReadyForWrite(nodeData.fullData);

                    bridge.writeFullNodeData(nodeData);
                }

                if (nodeData.hist != null) {
                    clearHistoryCache();

                    for (int i = 0, len = nodeData.hist.length; i < len; i++) {
                        DistributedMetaStorageHistoryItem histItem = nodeData.hist[i];

                        addToHistoryCache(ver.id() + i - (len - 1), histItem);
                    }
                }

                if (isPersistenceEnabled && nodeData.fullData != null)
                    worker.update(nodeData);

                if (nodeData.updates != null) {
                    for (DistributedMetaStorageHistoryItem update : nodeData.updates)
                        completeWrite(update);
                }
            }
            else if (!isClient && ver.id() > 0) {
                throw new IgniteException("Cannot join the cluster because it doesn't support distributed metastorage" +
                    " feature and this node has not empty distributed metastorage data");
            }
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException("Cannot join the cluster", ex);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Common implementation for {@link #write(String, Serializable)} and {@link #remove(String)}. Synchronously waits
     * for operation to be completed.
     *
     * @param key The key.
     * @param valBytes Value bytes to write. Null if value needs to be removed.
     * @throws IgniteCheckedException If there was an error while sending discovery message.
     */
    private GridFutureAdapter<?> startWrite(String key, byte[] valBytes) throws IgniteCheckedException {
       if (!isSupported(ctx))
            throw new IgniteCheckedException(NOT_SUPPORTED_MSG);

        UUID reqId = UUID.randomUUID();

        GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

        updateFuts.put(reqId, fut);

        DiscoveryCustomMessage msg = new DistributedMetaStorageUpdateMessage(reqId, key, valBytes);

        ctx.discovery().sendCustomEvent(msg);

        return fut;
    }

    /**
     * Basically the same as {@link #startWrite(String, byte[])} but for CAS operations.
     */
    private GridFutureAdapter<Boolean> startCas(String key, byte[] expValBytes, byte[] newValBytes)
        throws IgniteCheckedException {
         if (!isSupported(ctx))
            throw new IgniteCheckedException(NOT_SUPPORTED_MSG);

        UUID reqId = UUID.randomUUID();

        GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

        updateFuts.put(reqId, fut);

        DiscoveryCustomMessage msg = new DistributedMetaStorageCasMessage(reqId, key, expValBytes, newValBytes);

        ctx.discovery().sendCustomEvent(msg);

        return fut;
    }

    /**
     * Invoked when {@link DistributedMetaStorageUpdateMessage} received. Attempts to store received data (depends on
     * current {@link #bridge} value). Invokes failure handler with critical error if attempt failed for some reason.
     *
     * @param topVer Ignored.
     * @param node Ignored.
     * @param msg Received message.
     */
    private void onUpdateMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateMessage msg
    ) {
        if (msg.errorMessage() != null)
            return;

        if (!isSupported(ctx)) {
            msg.errorMessage(NOT_SUPPORTED_MSG);

            return;
        }

        try {
            if (msg instanceof DistributedMetaStorageCasMessage)
                completeCas((DistributedMetaStorageCasMessage)msg);
            else
                completeWrite(new DistributedMetaStorageHistoryItem(msg.key(), msg.value()));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw U.convertException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw criticalError(e);
        }
    }

    /**
     * Invoked when {@link DistributedMetaStorageUpdateAckMessage} received. Completes future if local node is the node
     * that initiated write operation.
     *
     * @param topVer Ignored.
     * @param node Ignored.
     * @param msg Received message.
     */
    private void onAckMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateAckMessage msg
    ) {
        GridFutureAdapter<Boolean> fut = updateFuts.remove(msg.requestId());

        if (fut != null) {
            String errorMsg = msg.errorMessage();

            if (errorMsg == null) {
                Boolean res = msg instanceof DistributedMetaStorageCasAckMessage
                    ? ((DistributedMetaStorageCasAckMessage)msg).updated()
                    : null;

                fut.onDone(res);
            }
            else
                fut.onDone(new IllegalStateException(errorMsg));
        }
    }

    /**
     * Invoke failure handler and rethrow passed exception, possibly wrapped into the unchecked one.
     */
    private RuntimeException criticalError(Throwable e) {
        ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

        if (e instanceof Error)
            throw (Error)e;

        throw U.convertException((IgniteCheckedException)e);
    }

    /**
     * Store data in local metastorage or in memory.
     *
     * @param histItem {@code <key, value>} pair to process.
     * @throws IgniteCheckedException In case of IO/unmarshalling errors.
     */
    private void completeWrite(
        DistributedMetaStorageHistoryItem histItem
    ) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            histItem = optimizeHistoryItem(histItem);

            if (histItem == null)
                return;

            ver = ver.nextVersion(histItem);

            for (int i = 0, len = histItem.keys().length; i < len; i++)
                notifyListeners(histItem.keys()[i], bridge.read(histItem.keys()[i]), unmarshal(marshaller, histItem.valuesBytesArray()[i]));

            for (int i = 0, len = histItem.keys().length; i < len; i++)
                bridge.write(histItem.keys()[i], histItem.valuesBytesArray()[i]);

            addToHistoryCache(ver.id(), histItem);
        }
        finally {
            lock.writeLock().unlock();
        }

        if (isPersistenceEnabled)
            worker.update(histItem);

        shrinkHistory();
    }

    /**
     * Remove updates that match already existing values.
     *
     * @param histItem New history item.
     * @return Updated history item or null is resulting history item turned out to be empty.
     */
    @Nullable private DistributedMetaStorageHistoryItem optimizeHistoryItem(
        DistributedMetaStorageHistoryItem histItem
    ) {
        String[] keys = histItem.keys();
        byte[][] valBytesArr = histItem.valuesBytesArray();

        int len = keys.length;
        int cnt = 0;

        BitSet matches = new BitSet(len);

        for (int i = 0; i < len; i++) {
            String key = keys[i];
            byte[] valBytes = valBytesArr[i];
            byte[] existingValBytes = bridge.readMarshalled(key);

            if (Arrays.equals(valBytes, existingValBytes))
                matches.set(i);
            else
                ++cnt;
        }

        if (cnt == 0)
            return null;

        if (cnt != len) {
            String[] newKeys = new String[cnt];
            byte[][] newValBytesArr = new byte[cnt][];

            for (int src = 0, dst = 0; src < len; src++) {
                if (!matches.get(src)) {
                    newKeys[dst] = keys[src];
                    newValBytesArr[dst] = valBytesArr[src];

                    ++dst;
                }
            }

            return new DistributedMetaStorageHistoryItem(newKeys, newValBytesArr);
        }

        return histItem;
    }

    /**
     * Store data in local metastorage or in memory.
     *
     * @param msg Message with all required data.
     * @see #completeWrite(DistributedMetaStorageHistoryItem)
     */
    private void completeCas(
        DistributedMetaStorageCasMessage msg
    ) throws IgniteCheckedException {
        if (!msg.matches())
            return;

        Serializable oldVal = bridge.read(msg.key());

        Serializable expVal = unmarshal(marshaller, msg.expectedValue());

        if (!Objects.deepEquals(oldVal, expVal)) {
            msg.setMatches(false);

            // Do nothing if expected value doesn't match with the actual one.
            return;
        }

        completeWrite(new DistributedMetaStorageHistoryItem(msg.key(), msg.value()));
    }

    /**
     * Store current update into the in-memory history cache.
     *
     * @param ver Version for the update.
     * @param histItem Update itself.
     */
    void addToHistoryCache(long ver, DistributedMetaStorageHistoryItem histItem) {
        assert lock.isWriteLockedByCurrentThread();

        if (!isClient)
            histCache.put(ver, histItem);
    }

    /**
     * Clear in-memory history cache.
     */
    void clearHistoryCache() {
        assert lock.isWriteLockedByCurrentThread();

        histCache.clear();
    }

    /**
     * Shrikn history so that its estimating size doesn't exceed {@link #histMaxBytes}.
     */
    private void shrinkHistory() {
        lock.writeLock().lock();

        try {
            while (histCache.sizeInBytes() > histMaxBytes && histCache.size() > 1) {
                histCache.removeOldest();

                if (isPersistenceEnabled)
                    worker.removeHistItem(ver.id() - histCache.size());
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Notify listeners on node start. Even if there was no data restoring.
     *
     * @param newData Data about which listeners should be notified.
     */
    private void notifyListenersBeforeReadyForWrite(
        DistributedMetaStorageKeyValuePair[] newData
    ) throws IgniteCheckedException {
        assert lock.isWriteLockedByCurrentThread();

        DistributedMetaStorageKeyValuePair[] oldData = bridge.localFullData();

        int oldIdx = 0, newIdx = 0;

        while (oldIdx < oldData.length && newIdx < newData.length) {
            String oldKey = oldData[oldIdx].key;
            byte[] oldValBytes = oldData[oldIdx].valBytes;

            String newKey = newData[newIdx].key;
            byte[] newValBytes = newData[newIdx].valBytes;

            int c = oldKey.compareTo(newKey);

            if (c < 0) {
                notifyListeners(oldKey, unmarshal(marshaller, oldValBytes), null);

                ++oldIdx;
            }
            else if (c > 0) {
                notifyListeners(newKey, null, unmarshal(marshaller, newValBytes));

                ++newIdx;
            }
            else {
                Serializable oldVal = unmarshal(marshaller, oldValBytes);

                Serializable newVal = Arrays.equals(oldValBytes, newValBytes) ? oldVal : unmarshal(marshaller, newValBytes);

                notifyListeners(oldKey, oldVal, newVal);

                ++oldIdx;

                ++newIdx;
            }
        }

        for (; oldIdx < oldData.length; ++oldIdx)
            notifyListeners(oldData[oldIdx].key, unmarshal(marshaller, oldData[oldIdx].valBytes), null);

        for (; newIdx < newData.length; ++newIdx)
            notifyListeners(newData[newIdx].key, null, unmarshal(marshaller, newData[newIdx].valBytes));
    }

    /**
     * Notify listeners.
     *
     * @param key The key.
     * @param oldVal Old value.
     * @param newVal New value.
     */
    private void notifyListeners(String key, Serializable oldVal, Serializable newVal) {
        for (IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>> entry : lsnrs) {
            if (entry.get1().test(key)) {
                try {
                    // ClassCastException might be thrown here for crappy listeners.
                    entry.get2().onUpdate(key, oldVal, newVal);
                }
                catch (Exception e) {
                    log.error(S.toString(
                        "Failed to notify distributed metastorage update listener",
                        "key", key, false,
                        "oldVal", oldVal, false,
                        "newVal", newVal, false,
                        "lsnr", entry.get2(), false
                    ), e);
                }
            }
        }
    }

    /** Checkpoint read lock. */
    private void localMetastorageLock() {
        ctx.cache().context().database().checkpointReadLock();
    }

    /** Checkpoint read unlock. */
    private void localMetastorageUnlock() {
        ctx.cache().context().database().checkpointReadUnlock();
    }
}
