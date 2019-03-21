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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
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
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
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
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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

/**
 * Implementation of {@link DistributedMetaStorage} based on {@link MetaStorage} for persistence and discovery SPI for
 * communication.
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

    /** */
    private final boolean isClient;

    /** */
    private final boolean isPersistenceEnabled;

    /** Cached subscription processor instance. Exists to make code shorter. */
    private final GridInternalSubscriptionProcessor isp; // Short name to avoid long lines.

    /** Bridge. Has some "phase-specific" code. Exists to avoid countless {@code if}s in code. */
    private volatile DistributedMetaStorageBridge bridge = new NotAvailableDistributedMetaStorageBridge();

    /**
     * {@link MetastorageLifecycleListener#onReadyForReadWrite(ReadWriteMetastorage)} is invoked asynchronously after
     * cluster activation so there's a chance of a gap where someone alreasy tries to write data but distributed
     * metastorage is not "writeable". Current latch aims to resolve this issue - every "write" action waits for it
     * before actually trying to write anything.
     */
    private volatile CountDownLatch writeAvailable = new CountDownLatch(1);

    /**
     * Version of distributed metastorage.
     */
    private volatile DistributedMetaStorageVersion ver;

    /** Listeners set. */
    final Set<IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>>> lsnrs =
        new GridConcurrentLinkedHashSet<>();

    /**
     * Map that contains latest changes in distributed metastorage. There should be no gaps in versions and the latest
     * version is always present in the map. This means that the map is empty only if version is 0.
     */
    private final DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

    /**
     * Maximal acceptable value of {@link #histCache} in bytes. After every write history would shrink until its size
     * is not greater then given value.
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
     * Some extra values that are useful only when node is not active. Otherwise it is nullized to remove excessive data
     * from the heap.
     *
     * @see StartupExtras
     */
    private volatile StartupExtras startupExtras = new StartupExtras();

    /**
     * Lock to access/update {@link #bridge} and {@link #startupExtras} fields (probably some others as well).
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Becomes {@code true} if node was deactivated, this information is useful for joining node validation.
     *
     * @see #validateNode(ClusterNode, DiscoveryDataBag.JoiningNodeDiscoveryData)
     */
    private boolean wasDeactivated;

    /**
     * Context marshaller.
     */
    public final JdkMarshaller marshaller;

    /**
     * @param ctx Kernal context.
     */
    public DistributedMetaStorageImpl(GridKernalContext ctx) {
        super(ctx);

        isClient = ctx.clientNode();

        isPersistenceEnabled = !isClient && isPersistenceEnabled(ctx.config());

        isp = ctx.internalSubscriptionProcessor();

        marshaller = ctx.marshallerContext().jdkMarshaller();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isPersistenceEnabled) {
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
                ) throws IgniteCheckedException {
                    onMetaStorageReadyForWrite(metastorage);
                }
            });
        }
        else {
            ver = INITIAL_VERSION;

            bridge = new EmptyDistributedMetaStorageBridge();
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

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (!isPersistenceEnabled) {
            for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
                subscriber.onReadyForRead(this);
        }

        if (active)
            onActivate(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (!isPersistenceEnabled) {
            lock.writeLock().lock();

            try {
                if (!(bridge instanceof InMemoryCachedDistributedMetaStorageBridge)) {
                    assert startupExtras != null;

                    InMemoryCachedDistributedMetaStorageBridge memCachedBridge =
                        new InMemoryCachedDistributedMetaStorageBridge(this);

                    memCachedBridge.restore(startupExtras);

                    executeDeferredUpdates(memCachedBridge);

                    bridge = memCachedBridge;

                    startupExtras = null;
                }
            }
            finally {
                lock.writeLock().unlock();
            }

            for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
                subscriber.onReadyForWrite(this);

            writeAvailable.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (isClient)
            return;

        lock.writeLock().lock();

        try {
            wasDeactivated = true;

            if (isPersistenceEnabled) {
                try {
                    DistributedMetaStorageKeyValuePair[] locFullData = bridge.localFullData();

                    bridge = new ReadOnlyDistributedMetaStorageBridge(marshaller, locFullData);
                }
                catch (IgniteCheckedException e) {
                    throw criticalError(e);
                }

                startupExtras = new StartupExtras();
            }

            if (writeAvailable.getCount() > 0)
                writeAvailable.countDown();

            writeAvailable = new CountDownLatch(1);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** Whether cluster is active at this moment or not. Also returns {@code true} if cluster is being activated. */
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

        assert startupExtras != null;

        ReadOnlyDistributedMetaStorageBridge readOnlyBridge = new ReadOnlyDistributedMetaStorageBridge(marshaller);

        lock();

        try {
            ver = readOnlyBridge.readInitialData(metastorage, startupExtras);

            metastorage.iterate(
                historyItemPrefix(),
                (key, val) -> addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                true
            );
        }
        finally {
            unlock();
        }

        bridge = readOnlyBridge;

        for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
            subscriber.onReadyForRead(this);
    }

    /**
     * Implementation for {@link MetastorageLifecycleListener#onReadyForReadWrite(ReadWriteMetastorage)} listener.
     * Invoked after each activation (only in persistent clusters).
     *
     * @param metastorage Local metastorage instance available for writing.
     * @throws IgniteCheckedException If there were any errors while accessing local metastorage.
     * @see MetastorageLifecycleListener#onReadyForReadWrite(ReadWriteMetastorage)
     */
    private void onMetaStorageReadyForWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        assert isPersistenceEnabled;

        lock.writeLock().lock();

        try {
            WritableDistributedMetaStorageBridge writableBridge = new WritableDistributedMetaStorageBridge(this, metastorage);

            if (startupExtras != null) {
                lock();

                try {
                    writableBridge.restore(startupExtras);
                }
                finally {
                    unlock();
                }

                executeDeferredUpdates(writableBridge);
            }

            bridge = writableBridge;

            startupExtras = null;
        }
        finally {
            lock.writeLock().unlock();
        }

        for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
            subscriber.onReadyForWrite(this);

        writeAvailable.countDown();
    }

    /** {@inheritDoc} */
    @Override public long getUpdatesCount() {
        return bridge instanceof ReadOnlyDistributedMetaStorageBridge
            ? ((ReadOnlyDistributedMetaStorageBridge)bridge).version().id
            : ver.id;
    }

    /** {@inheritDoc} */
    @Override @Nullable public <T extends Serializable> T read(@NotNull String key) throws IgniteCheckedException {
        lock();

        try {
            return (T)bridge.read(key, true);
        }
        finally {
            unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException {
        assert val != null : key;

        startWrite(key, marshal(marshaller, val)).get();
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> writeAsync(@NotNull String key, @NotNull Serializable val)
        throws IgniteCheckedException {
        assert val != null : key;

        return startWrite(key, marshal(marshaller, val));
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
        lock();

        try {
            bridge.iterate(keyPrefix, cb, true);
        }
        finally {
            unlock();
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
    @Override public Long collectTcpHandshakeResponseData() {
        lock.readLock().lock();

        try {
            long ver = getActualVersion().id;

            return ver == 0L ? null : ver;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onTcpHandshakeResponseDataReceived(Serializable componentData) {
        lock.writeLock().lock();

        try {
            assert startupExtras != null;

            if (isClient)
                startupExtras = new StartupExtras();

            if (componentData != null) {
                assert componentData instanceof Long;

                startupExtras.verFromDiscoveryClusterData = (Long)componentData;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        lock.readLock().lock();

        try {
            if (isClient) {
                Serializable data = new DistributedMetaStorageJoiningNodeData(
                    getBaselineTopologyId(),
                    getActualVersion(),
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

            assert startupExtras != null;

            DistributedMetaStorageVersion verToSnd = bridge instanceof ReadOnlyDistributedMetaStorageBridge
                ? ((ReadOnlyDistributedMetaStorageBridge)bridge).version()
                : ver;

            long clusterVer = startupExtras.verFromDiscoveryClusterData;

            DistributedMetaStorageHistoryItem[] hist;

            if (verToSnd.id <= clusterVer)
                hist = EMPTY_ARRAY;
            else if (clusterVer + histCache.size() < verToSnd.id)
                hist = histCache.toArray();
            else
                hist = Arrays.copyOfRange(histCache.toArray(), (int)(clusterVer + histCache.size() - verToSnd.id), histCache.size());

            Serializable data = new DistributedMetaStorageJoiningNodeData(
                getBaselineTopologyId(),
                verToSnd,
                hist
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

    /** Returns current baseline topology id of {@code -1} if there's no baseline topology found. */
    private int getBaselineTopologyId() {
        BaselineTopology baselineTop = ctx.state().clusterState().baselineTopology();

        return baselineTop != null ? baselineTop.id() : -1;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteNodeValidationResult validateNode(
        ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData
    ) {
        if (isClient)
            return null;

        lock.readLock().lock();

        try {
            DistributedMetaStorageVersion locVer = getActualVersion();

            if (!discoData.hasJoiningNodeData()) {
                // Joining node doesn't support distributed metastorage feature.

                if (isSupported(ctx) && locVer.id > 0 && !(node.isClient() || node.isDaemon())) {
                    String errorMsg = "Node not supporting distributed metastorage feature" +
                        " is not allowed to join the cluster";

                    return new IgniteNodeValidationResult(node.id(), errorMsg, errorMsg);
                }
                else
                    return null;
            }

            DistributedMetaStorageJoiningNodeData joiningData = getJoiningNodeData(discoData);

            if (joiningData == null) {
                String errorMsg = "Cannot unmarshal joining node data";

                return new IgniteNodeValidationResult(node.id(), errorMsg, errorMsg);
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

            int locHistSize = getAvailableHistorySize();

            if (remoteVer.id < locVer.id - locHistSize) {
                // Remote node is too far behind.
                // Technicaly this situation should be banned because there's no way to prove data consistency.
                errorMsg = null;
            }
            else if (remoteVer.id < locVer.id) {
                // Remote node it behind the cluster version and there's enough history.
                DistributedMetaStorageVersion newRemoteVer = remoteVer.nextVersion(
                    this::historyItem,
                    remoteVer.id + 1,
                    locVer.id
                );

                if (!newRemoteVer.equals(locVer))
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
                else
                    errorMsg = null;
            }
            else if (remoteVer.id == locVer.id) {
                // Remote and local versions match.
                if (!remoteVer.equals(locVer)) {
                    errorMsg = S.toString(
                        "Joining node has conflicting distributed metastorage data:",
                        "clusterVersion", locVer, false,
                        "joiningNodeVersion", remoteVer, false
                    );
                }
                else
                    errorMsg = null;
            }
            else if (remoteVer.id <= locVer.id + remoteHistSize) {
                // Remote node is ahead of the cluster and has enough history.
                if (clusterIsActive) {
                    errorMsg = "Attempting to join node with larger distributed metastorage version id." +
                        " The node is most likely in invalid state and can't be joined.";
                }
                else if (wasDeactivated || remoteBltId < locBltId)
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
                else {
                    DistributedMetaStorageVersion newLocVer = locVer.nextVersion(
                        remoteHist,
                        remoteHistSize - (int)(remoteVer.id - locVer.id),
                        remoteHistSize
                    );

                    if (!newLocVer.equals(remoteVer))
                        errorMsg = "Joining node has conflicting distributed metastorage data.";
                    else
                        errorMsg = null;
                }
            }
            else {
                assert remoteVer.id > locVer.id + remoteHistSize;

                // Remote node is too far ahead.
                if (clusterIsActive) {
                    errorMsg = "Attempting to join node with larger distributed metastorage version id." +
                        " The node is most likely in invalid state and can't be joined.";
                }
                else if (wasDeactivated || remoteBltId < locBltId)
                    errorMsg = "Joining node has conflicting distributed metastorage data.";
                else {
                    errorMsg = "Joining node doesn't have enough history items in distributed metastorage data." +
                        " Please check the order in which you start cluster nodes.";
                }
            }

            return (errorMsg == null) ? null : new IgniteNodeValidationResult(node.id(), errorMsg, errorMsg);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        if (!discoData.hasJoiningNodeData())
            return;

        DistributedMetaStorageJoiningNodeData joiningData = getJoiningNodeData(discoData);

        if (joiningData == null)
            return;

        DistributedMetaStorageVersion remoteVer = joiningData.ver;

        if (!isSupported(ctx) && remoteVer.id > 0)
            return;

        lock.writeLock().lock();

        try {
            DistributedMetaStorageVersion actualVer = getActualVersion();

            if (remoteVer.id > actualVer.id) {
                assert startupExtras != null;

                DistributedMetaStorageHistoryItem[] hist = joiningData.hist;

                if (remoteVer.id - actualVer.id <= hist.length) {
                    assert bridge instanceof ReadOnlyDistributedMetaStorageBridge
                        || bridge instanceof EmptyDistributedMetaStorageBridge;

                    for (long v = actualVer.id + 1; v <= remoteVer.id; v++)
                        updateLater(hist[(int)(v - remoteVer.id + hist.length - 1)]);
                }
                else
                    assert false : "Joining node is too far ahead [remoteVer=" + remoteVer + "]";
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (isClient)
            return;

        if (dataBag.commonDataCollectedFor(COMPONENT_ID))
            return;

        DiscoveryDataBag.JoiningNodeDiscoveryData discoData = dataBag.newJoinerDiscoveryData(COMPONENT_ID);

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
            //TODO Store it precalculated? Maybe later.
            DistributedMetaStorageVersion actualVer = getActualVersion();

            if (remoteVer.id > actualVer.id) {
                Serializable nodeData = new DistributedMetaStorageClusterNodeData(remoteVer, null, null, null);

                dataBag.addGridCommonData(COMPONENT_ID, nodeData);
            }
            else {
                if (remoteVer.id == actualVer.id) {
                    Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver, null, null, null);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
                else {
                    int availableHistSize = getAvailableHistorySize();

                    if (actualVer.id - remoteVer.id <= availableHistSize) {
                        DistributedMetaStorageHistoryItem[] hist = history(remoteVer.id + 1, actualVer.id);

                        Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver, null, null, hist);

                        dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                    }
                    else {
                        DistributedMetaStorageVersion ver0;

                        DistributedMetaStorageKeyValuePair[] fullData;

                        DistributedMetaStorageHistoryItem[] hist;

                        if (startupExtras == null || startupExtras.fullNodeData == null) {
                            ver0 = ver;

                            try {
                                fullData = bridge.localFullData();
                            }
                            catch (IgniteCheckedException e) {
                                throw criticalError(e);
                            }

                            hist = history(ver.id - histCache.size() + 1, actualVer.id);
                        }
                        else {
                            ver0 = startupExtras.fullNodeData.ver;

                            fullData = startupExtras.fullNodeData.fullData;

                            hist = startupExtras.fullNodeData.hist;
                        }

                        DistributedMetaStorageHistoryItem[] updates;

                        if (startupExtras != null)
                            updates = startupExtras.deferredUpdates.toArray(EMPTY_ARRAY);
                        else
                            updates = null;

                        Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver0, fullData, hist, updates);

                        dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                    }
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** */
    @Nullable private DistributedMetaStorageJoiningNodeData getJoiningNodeData(
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData
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
            startupExtras = new StartupExtras();

            bridge = new EmptyDistributedMetaStorageBridge();

            if (writeAvailable.getCount() > 0)
                writeAvailable.countDown();

            writeAvailable = new CountDownLatch(1);

            ver = INITIAL_VERSION;

            wasDeactivated = false;

            for (GridFutureAdapter<Boolean> fut : updateFuts.values())
                fut.onDone(new IgniteCheckedException("Client was disconnected during the operation."));

            updateFuts.clear();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        assert isClient;

        if (isActive())
            onActivate(ctx);

        return null;
    }

    /**
     * Returns number of all available history items. Might be a history from remote node snapshot or/and deferred
     * updates from another remote node. Depends on the current node state.
     */
    private int getAvailableHistorySize() {
        if (startupExtras == null)
            return histCache.size();
        else if (startupExtras.fullNodeData == null)
            return histCache.size() + startupExtras.deferredUpdates.size();
        else
            return startupExtras.fullNodeData.hist.length + startupExtras.deferredUpdates.size();
    }

    /**
     * Returns actual version from the local node. It is just a version for activated node or calculated future version
     * otherwise.
     */
    private DistributedMetaStorageVersion getActualVersion() {
        if (startupExtras == null)
            return ver;
        else if (startupExtras.fullNodeData == null)
            return ver.nextVersion(startupExtras.deferredUpdates);
        else
            return startupExtras.fullNodeData.ver.nextVersion(startupExtras.deferredUpdates);
    }

    /**
     * Returns last update for the specified version.
     *
     * @param specificVer Specific version.
     * @return {@code <key, value>} pair if it was found, {@code null} otherwise.
     */
    private DistributedMetaStorageHistoryItem historyItem(long specificVer) {
        if (startupExtras == null)
            return histCache.get(specificVer);
        else {
            DistributedMetaStorageClusterNodeData fullNodeData = startupExtras.fullNodeData;

            long notDeferredVer;

            if (fullNodeData == null) {
                notDeferredVer = ver.id;

                if (specificVer <= notDeferredVer)
                    return histCache.get(specificVer);
            }
            else {
                notDeferredVer = fullNodeData.ver.id;

                if (specificVer <= notDeferredVer) {
                    int idx = (int)(specificVer - notDeferredVer + fullNodeData.hist.length - 1);

                    return idx >= 0 ? fullNodeData.hist[idx] : null;
                }
            }

            assert specificVer > notDeferredVer;

            int idx = (int)(specificVer - notDeferredVer - 1);

            List<DistributedMetaStorageHistoryItem> deferredUpdates = startupExtras.deferredUpdates;

            if (idx < deferredUpdates.size())
                return deferredUpdates.get(idx);

            return null;
        }
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
     * {@link DistributedMetaStorageBridge#localFullData()} invoked on {@link #bridge}.
     */
    @TestOnly
    private DistributedMetaStorageKeyValuePair[] localFullData() throws IgniteCheckedException {
        return bridge.localFullData();
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        lock.writeLock().lock();

        try {
            DistributedMetaStorageClusterNodeData nodeData = (DistributedMetaStorageClusterNodeData)data.commonData();

            if (nodeData != null) {
                if (nodeData.fullData == null) {
                    if (nodeData.updates != null) {
                        for (DistributedMetaStorageHistoryItem update : nodeData.updates)
                            updateLater(update);
                    }
                }
                else
                    writeFullDataLater(nodeData);
            }
            else if (!isClient && getActualVersion().id > 0) {
                throw new IgniteException("Cannot join the cluster because it doesn't support distributed metastorage" +
                    " feature and this node has not empty distributed metastorage data");
            }
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
     * @throws IgniteCheckedException If there was an error while sending discovery message or message was sent but
     * cluster is not active.
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

        if (!isActive()) {
            msg.errorMessage("Ignite cluster is not active");

            return;
        }

        if (!isSupported(ctx)) {
            msg.errorMessage(NOT_SUPPORTED_MSG);

            return;
        }

        try {
            U.await(writeAvailable);

            if (msg instanceof DistributedMetaStorageCasMessage)
                completeCas(bridge, (DistributedMetaStorageCasMessage)msg);
            else
                completeWrite(bridge, new DistributedMetaStorageHistoryItem(msg.key(), msg.value()), false, true);
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
     * @param bridge Bridge to get the access to the storage.
     * @param histItem {@code <key, value>} pair to process.
     * @param force If {@code true} then apply all updates even if data is the same.
     * @param notifyListeners Whether listeners should be notified or not. {@code false} for data restore on
     * activation.
     * @throws IgniteCheckedException In case of IO/unmarshalling errors.
     */
    private void completeWrite(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageHistoryItem histItem,
        boolean force,
        boolean notifyListeners
    ) throws IgniteCheckedException {
        lock();

        try {
            if (!force) {
                histItem = optimizeHistoryItem(bridge, histItem);

                if (histItem == null)
                    return;
            }

            bridge.onUpdateMessage(histItem);

            if (notifyListeners)
                for (int i = 0, len = histItem.keys.length; i < len; i++)
                    notifyListeners(histItem.keys[i], bridge.read(histItem.keys[i], true), unmarshal(marshaller, histItem.valBytesArray[i]));

            for (int i = 0, len = histItem.keys.length; i < len; i++)
                bridge.write(histItem.keys[i], histItem.valBytesArray[i]);
        }
        finally {
            unlock();
        }

        addToHistoryCache(ver.id, histItem);

        shrinkHistory(bridge);
    }

    /**
     * Remove updates that match already existing values.
     *
     * @param bridge Bridge for data access.
     * @param histItem New history item.
     * @return Updated history item or null is resulting history item turned out to be empty.
     * @throws IgniteCheckedException In case of IO/unmarshalling errors.
     */
    @Nullable private DistributedMetaStorageHistoryItem optimizeHistoryItem(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageHistoryItem histItem
    ) throws IgniteCheckedException {
        String[] keys = histItem.keys;
        byte[][] valBytesArr = histItem.valBytesArray;

        int len = keys.length;
        int cnt = 0;

        BitSet matches = new BitSet(len);

        for (int i = 0; i < len; i++) {
            String key = keys[i];
            byte[] valBytes = valBytesArr[i];
            byte[] existingValBytes = (byte[])bridge.read(key, false);

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
     * @param bridge Bridge to get the access to the storage.
     * @param msg Message with all required data.
     * @see #completeWrite(DistributedMetaStorageBridge, DistributedMetaStorageHistoryItem, boolean, boolean)
     */
    private void completeCas(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageCasMessage msg
    ) throws IgniteCheckedException {
        if (!msg.matches())
            return;

        lock();

        try {
            Serializable oldVal = bridge.read(msg.key(), true);

            Serializable expVal = unmarshal(marshaller, msg.expectedValue());

            if (!Objects.deepEquals(oldVal, expVal)) {
                msg.setMatches(false);

                // Do nothing if expected value doesn't match with the actual one.
                return;
            }
        }
        finally {
            unlock();
        }

        completeWrite(bridge, new DistributedMetaStorageHistoryItem(msg.key(), msg.value()), false, true);
    }

    /**
     * Store current update into the in-memory history cache.
     *
     * @param ver Version for the update.
     * @param histItem Update itself.
     */
    void addToHistoryCache(long ver, DistributedMetaStorageHistoryItem histItem) {
        if (!isClient)
            histCache.put(ver, histItem);
    }

    /**
     * Clear in-memory history cache.
     */
    void clearHistoryCache() {
        histCache.clear();
    }

    /**
     * Shrikn history so that its estimating size doesn't exceed {@link #histMaxBytes}.
     */
    private void shrinkHistory(
        DistributedMetaStorageBridge bridge
    ) throws IgniteCheckedException {
        long maxBytes = histMaxBytes;

        if (histCache.sizeInBytes() > maxBytes && histCache.size() > 1) {
            lock();

            try {
                while (histCache.sizeInBytes() > maxBytes && histCache.size() > 1) {
                    bridge.removeHistoryItem(ver.id + 1 - histCache.size());

                    histCache.removeOldest();
                }
            }
            finally {
                unlock();
            }
        }
    }

    /**
     * Add update into the list of deferred updates. Works for inactive nodes only.
     */
    private void updateLater(DistributedMetaStorageHistoryItem update) {
        assert startupExtras != null;

        startupExtras.deferredUpdates.add(update);
    }

    /**
     * Invoked at the end of activation.
     *
     * @param bridge Bridge to access data storage.
     * @throws IgniteCheckedException In case of IO/unmarshalling errors.
     */
    private void executeDeferredUpdates(DistributedMetaStorageBridge bridge) throws IgniteCheckedException {
        assert startupExtras != null;

        DistributedMetaStorageHistoryItem lastUpdate = histCache.get(ver.id);

        if (lastUpdate != null) {
            lock();

            try {
                boolean checkDataEquality = true;

                for (int i = 0, len = lastUpdate.keys.length; i < len; i++) {
                    String lastUpdateKey = lastUpdate.keys[i];
                    byte[] lastUpdateValBytes = lastUpdate.valBytesArray[i];

                    boolean write;

                    if (!checkDataEquality)
                        write = true;
                    else {
                        byte[] existingValBytes = (byte[])bridge.read(lastUpdateKey, false);

                        write = !Arrays.equals(existingValBytes, lastUpdateValBytes);

                        checkDataEquality = false;
                    }

                    if (write)
                        bridge.write(lastUpdateKey, lastUpdateValBytes);
                }
            }
            finally {
                unlock();
            }
        }

        for (DistributedMetaStorageHistoryItem histItem : startupExtras.deferredUpdates)
            completeWrite(bridge, histItem, true, false);

        notifyListenersBeforeReadyForWrite(bridge);
    }

    /**
     * Notify listeners at the end of activation. Even if there was no data restoring.
     *
     * @param bridge Bridge to access data storage.
     */
    private void notifyListenersBeforeReadyForWrite(
        DistributedMetaStorageBridge bridge
    ) throws IgniteCheckedException {
        DistributedMetaStorageKeyValuePair[] oldData = this.bridge.localFullData();

        DistributedMetaStorageKeyValuePair[] newData = bridge.localFullData();

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
     * Ultimate version of {@link #updateLater(DistributedMetaStorageHistoryItem)}.
     *
     * @param nodeData Data received from remote node.
     */
    private void writeFullDataLater(DistributedMetaStorageClusterNodeData nodeData) {
        assert nodeData.fullData != null;

        startupExtras.fullNodeData = nodeData;

        startupExtras.deferredUpdates.clear();

        if (nodeData.updates != null) {
            for (DistributedMetaStorageHistoryItem update : nodeData.updates)
                updateLater(update);

            nodeData.updates = null;
        }
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
                        "Failed to notify global metastorage update listener",
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
    private void lock() {
        ctx.cache().context().database().checkpointReadLock();
    }

    /** Checkpoint read unlock. */
    private void unlock() {
        ctx.cache().context().database().checkpointReadUnlock();
    }

    /** */
    public DistributedMetaStorageVersion getVer() {
        return ver;
    }

    /** */
    public void setVer(DistributedMetaStorageVersion ver) {
        this.ver = ver;
    }
}
