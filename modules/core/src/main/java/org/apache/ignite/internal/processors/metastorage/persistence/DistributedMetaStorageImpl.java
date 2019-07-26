/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
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

    /**
     * Bridge. Has some "phase-specific" code. Exists to avoid countless {@code if}s in code.
     *
     * @see NotAvailableDistributedMetaStorageBridge
     * @see EmptyDistributedMetaStorageBridge
     * @see ReadOnlyDistributedMetaStorageBridge
     * @see InMemoryCachedDistributedMetaStorageBridge
     * @see WritableDistributedMetaStorageBridge
     */
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
     * @see #validateNode(ClusterNode, JoiningNodeDiscoveryData)
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

    /**
     * {@inheritDoc}
     * <br/>
     * Create all required listeners.
     */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

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

    /**
     * {@inheritDoc}
     * <br/>
     * Makes sense only if {@link #isPersistenceEnabled} is {@code false}. In this case metastorage will be declared as
     * {@code readyForRead} and then {@code readyForWrite} if {@code active} flag happened to be {@code true}.
     */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {

    }

    public void inMemoryReadyForRead() {
        if (!isPersistenceEnabled) {
            for (DistributedMetastorageLifecycleListener subscriber : isp.getDistributedMetastorageSubscribers())
                subscriber.onReadyForRead(this);
        }
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Makes sense only if {@link #isPersistenceEnabled} is {@code false}. In this case metastorage will be declared as
     * {@code readyForWrite}.
     */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }

    public void inMemoryReadyForWrite() throws IgniteCheckedException {
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

    /**
     * {@inheritDoc}
     * <br/>
     * Moves persistent metastorage into read-only state. Does nothing if metastorage is not persistent.q
     */
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

        assert startupExtras != null;

        ReadOnlyDistributedMetaStorageBridge readOnlyBridge = new ReadOnlyDistributedMetaStorageBridge(marshaller);

        localMetastorageLock();

        try {
            ver = readOnlyBridge.readInitialData(metastorage, startupExtras);

            metastorage.iterate(
                historyItemPrefix(),
                (key, val) -> addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                true
            );
        }
        finally {
            localMetastorageUnlock();
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
                localMetastorageLock();

                try {
                    writableBridge.restore(startupExtras);
                }
                finally {
                    localMetastorageUnlock();
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
        localMetastorageLock();

        try {
            return (T)bridge.read(key, true);
        }
        finally {
            localMetastorageUnlock();
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
        localMetastorageLock();

        try {
            bridge.iterate(keyPrefix, cb, true);
        }
        finally {
            localMetastorageUnlock();
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

            DistributedMetaStorageHistoryItem[] hist = histCache.toArray();

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
     *         Do not join node that has updates count greater then on local node and cluster is active.
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
            DistributedMetaStorageVersion locVer = getActualVersion();

            if (!discoData.hasJoiningNodeData()) {
                // Joining node doesn't support distributed metastorage feature.

                if (isSupported(ctx) && locVer.id > 0 && !(node.isClient() || node.isDaemon())) {
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
            for (int i = 0; i < item.keys.length; i++) {
                try {
                    unmarshal(marshaller, item.valBytesArray[i]);
                }
                catch (IgniteCheckedException e) {
                    return "Unable to unmarshal key=" + item.keys[i];
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
     * node has bigger distributed metastorage version, in this case all required updates will be added to deferred
     * updates queue.
     *
     * @see StartupExtras#deferredUpdates
     */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData discoData) {
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
            //TODO Store it precalculated? Maybe later.
            DistributedMetaStorageVersion actualVer = getActualVersion();

            if (remoteVer.id > actualVer.id) { // Impossible for client.
                Serializable nodeData = new DistributedMetaStorageClusterNodeData(remoteVer, null, null, null);

                dataBag.addGridCommonData(COMPONENT_ID, nodeData);
            }
            else if (remoteVer.id == actualVer.id) { // On client this would mean that cluster metastorage is empty.
                Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver, null, null, null);

                dataBag.addGridCommonData(COMPONENT_ID, nodeData);
            }
            else {
                int availableHistSize = getAvailableHistorySize();

                if (actualVer.id - remoteVer.id <= availableHistSize && !dataBag.isJoiningNodeClient()) {
                    DistributedMetaStorageHistoryItem[] updates = history(remoteVer.id + 1, actualVer.id);

                    Serializable nodeData = new DistributedMetaStorageClusterNodeData(ver, null, null, updates);

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

                        if (dataBag.isJoiningNodeClient())
                            hist = EMPTY_ARRAY;
                        else
                            hist = history(ver.id - histCache.size() + 1, actualVer.id);
                    }
                    else {
                        ver0 = startupExtras.fullNodeData.ver;

                        fullData = startupExtras.fullNodeData.fullData;

                        if (dataBag.isJoiningNodeClient())
                            hist = EMPTY_ARRAY;
                        else
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
        localMetastorageLock();

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
            localMetastorageUnlock();
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

        localMetastorageLock();

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
            localMetastorageUnlock();
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
            localMetastorageLock();

            try {
                while (histCache.sizeInBytes() > maxBytes && histCache.size() > 1) {
                    bridge.removeHistoryItem(ver.id + 1 - histCache.size());

                    histCache.removeOldest();
                }
            }
            finally {
                localMetastorageUnlock();
            }
        }
    }

    /**
     * Add update into the list of deferred updates. Works for inactive nodes only.
     */
    private void updateLater(DistributedMetaStorageHistoryItem update) {
        assert startupExtras != null;

        startupExtras.deferredUpdates.add(update);

        for (int i = 0; i < update.keys.length; i++) {
            try {
                notifyListeners(
                    update.keys[i],
                    bridge.read(update.keys[i], true),
                    unmarshal(marshaller, update.valBytesArray[i])
                );
            }
            catch (IgniteCheckedException ex) {
                log.error("Unable to unmarshal new metastore data. key=" + update.keys[i], ex);
            }
        }
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
            localMetastorageLock();

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
                localMetastorageUnlock();
            }
        }

        for (DistributedMetaStorageHistoryItem histItem : startupExtras.deferredUpdates)
            completeWrite(bridge, histItem, true, false);
    }

    /**
     * Notify listeners at the end of activation. Even if there was no data restoring.
     *
     * @param newData Data about which listeners should be notified.
     */
    private void notifyListenersBeforeReadyForWrite(
        DistributedMetaStorageKeyValuePair[] newData
    ) throws IgniteCheckedException {
        DistributedMetaStorageKeyValuePair[] oldData = this.bridge.localFullData();

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
    private void writeFullDataLater(
        @NotNull DistributedMetaStorageClusterNodeData nodeData
    ) throws IgniteCheckedException {
        assert nodeData.fullData != null;

        startupExtras.fullNodeData = nodeData;

        startupExtras.deferredUpdates.clear();

        notifyListenersBeforeReadyForWrite(nodeData.fullData);

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
    private void localMetastorageLock() {
        ctx.cache().context().database().checkpointReadLock();
    }

    /** Checkpoint read unlock. */
    private void localMetastorageUnlock() {
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
