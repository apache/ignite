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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.META_STORAGE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.marshal;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;

/** */
public class DistributedMetaStorageImpl extends GridProcessorAdapter
    implements DistributedMetaStorage, IgniteChangeGlobalStateSupport
{
    /** */
    private static final int COMPONENT_ID = META_STORAGE.ordinal();

    /** */
    private static final long DFLT_MAX_HISTORY_BYTES = 100 * 1024 * 1024;

    /** */
    final GridInternalSubscriptionProcessor subscrProcessor;

    /** */
    private DistributedMetaStorageBridge bridge = new NotAvailableDistributedMetaStorageBridge();

    /** */
    private CountDownLatch writeAvailable = new CountDownLatch(1);

    /** */
    DistributedMetaStorageVersion ver;

    /** */
    final Set<IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>>> lsnrs =
        new GridConcurrentLinkedHashSet<>();

    /** */ //TODO Change to java.util.ArrayDeque?
    private final Map<Long, DistributedMetaStorageHistoryItem> histCache = new ConcurrentHashMap<>();

    /** */
    private long histSizeApproximation;

    /** */
    private final long histMaxBytes = IgniteSystemProperties.getLong(
        IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES,
        DFLT_MAX_HISTORY_BYTES
    );

    /** */
    private final ConcurrentMap<UUID, GridFutureAdapter<Boolean>> updateFuts = new ConcurrentHashMap<>();

    /** */
    private StartupExtras startupExtras = new StartupExtras();

    /** */
    private final Object innerStateLock = new Object();

    /** */
    private boolean wasDeactivated;

    /**
     * @param ctx Kernal context.
     */
    public DistributedMetaStorageImpl(GridKernalContext ctx) {
        super(ctx);

        subscrProcessor = ctx.internalSubscriptionProcessor();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isPersistenceEnabled(ctx.config())) {
            subscrProcessor.registerMetastorageListener(new MetastorageLifecycleListener() {
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
            startupExtras.locFullData = EMPTY_ARRAY;

            startupExtras.verToSnd = ver = DistributedMetaStorageVersion.INITIAL_VERSION;

            bridge = new EmptyDistributedMetaStorageBridge();

            for (DistributedMetastorageLifecycleListener subscriber : subscrProcessor.getGlobalMetastorageSubscribers())
                subscriber.onReadyForRead(this);
        }

        ctx.discovery().setCustomEventListener(
            DistributedMetaStorageUpdateMessage.class,
            this::onUpdateMessage
        );

        ctx.discovery().setCustomEventListener(
            DistributedMetaStorageUpdateAckMessage.class,
            this::onAckMessage
        );
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (active)
            onActivate(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (!isPersistenceEnabled(ctx.config())) {
            if (!(bridge instanceof InMemoryCachedDistributedMetaStorageBridge)) {
                synchronized (innerStateLock) {
                    assert startupExtras != null;

                    InMemoryCachedDistributedMetaStorageBridge memCachedBridge =
                        new InMemoryCachedDistributedMetaStorageBridge(this);

                    memCachedBridge.restore(startupExtras);

                    executeDeferredUpdates(memCachedBridge);

                    bridge = memCachedBridge;

                    startupExtras = null;
                }
            }

            for (DistributedMetastorageLifecycleListener subscriber : subscrProcessor.getGlobalMetastorageSubscribers())
                subscriber.onReadyForWrite(this);

            writeAvailable.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        synchronized (innerStateLock) {
            wasDeactivated = true;

            if (isPersistenceEnabled(ctx.config())) {
                DistributedMetaStorageHistoryItem[] locFullData = localFullData(bridge);

                bridge = new ReadOnlyDistributedMetaStorageBridge(this, locFullData);

                startupExtras = new StartupExtras();

                startupExtras.locFullData = locFullData;
            }

            writeAvailable = new CountDownLatch(1);
        }
    }

    /** */
    private boolean isActive() {
        return ctx.state().clusterState().active();
    }

    /** */
    @NotNull private IllegalStateException clusterIsNotActiveException() {
        return new IllegalStateException("Ignite cluster is not active");
    }

    /** */
    private void onMetaStorageReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        assert isPersistenceEnabled(ctx.config());

        assert startupExtras != null;

        ReadOnlyDistributedMetaStorageBridge readOnlyBridge = new ReadOnlyDistributedMetaStorageBridge(this);

        lock();

        try {
            readOnlyBridge.readInitialData(metastorage, startupExtras);
        }
        finally {
            unlock();
        }

        DistributedMetaStorageBridge oldBridge = bridge;

        bridge = readOnlyBridge;

        for (DistributedMetastorageLifecycleListener subscriber : subscrProcessor.getGlobalMetastorageSubscribers())
            subscriber.onReadyForRead(this);

        bridge = oldBridge;
    }

    /** */
    private void onMetaStorageReadyForWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        assert isPersistenceEnabled(ctx.config());

        synchronized (innerStateLock) {
            WritableDistributedMetaStorageBridge writableBridge = new WritableDistributedMetaStorageBridge(this, metastorage);

            if (startupExtras != null) {
//                long metaStorageMaxSize = ctx.config().getDataStorageConfiguration().getSystemRegionMaxSize();
//
//                histMaxBytes = Math.min(histMaxBytes, metaStorageMaxSize / 2);

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

        for (DistributedMetastorageLifecycleListener subscriber : subscrProcessor.getGlobalMetastorageSubscribers())
            subscriber.onReadyForWrite(this);

        writeAvailable.countDown();
    }

    /** {@inheritDoc} */
    @Override @Nullable public <T extends Serializable> T read(@NotNull String key) throws IgniteCheckedException {
        lock();

        try {
            return (T)bridge.read(key);
        }
        finally {
            unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException {
        assert val != null : key;

        startWrite(key, marshal(val));
    }

    /** {@inheritDoc} */
    @Override public void remove(@NotNull String key) throws IgniteCheckedException {
        startWrite(key, null);
    }

    /** {@inheritDoc} */
    @Override public boolean casWrite(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException {
        assert newVal != null : key;

        return startCas(key, marshal(expVal), marshal(newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean casRemove(
        @NotNull String key,
        @NotNull Serializable expVal
    ) throws IgniteCheckedException {
        assert expVal != null : key;

        return startCas(key, marshal(expVal), null);
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
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        assert startupExtras != null;

        DistributedMetaStorageHistoryItem[] hist = new TreeMap<>(histCache) // Sorting might be avoided if histCache is a queue
            .values()
            .toArray(EMPTY_ARRAY);

        Serializable data = new DistributedMetaStorageJoiningNodeData(
            getBaselineTopologyId(),
            startupExtras.verToSnd,
            null,//startupExtras.locFullData,
            hist
        );

        dataBag.addJoiningNodeData(COMPONENT_ID, data);
    }

    /** */
    private int getBaselineTopologyId() {
        BaselineTopology baselineTop = ctx.state().clusterState().baselineTopology();

        return baselineTop != null ? baselineTop.id() : -1;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteNodeValidationResult validateNode(
        ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData
    ) {
        if (!discoData.hasJoiningNodeData() || !isPersistenceEnabled(ctx.config()))
            return null;

        DistributedMetaStorageJoiningNodeData joiningData =
            (DistributedMetaStorageJoiningNodeData)discoData.joiningNodeData();

        DistributedMetaStorageVersion remoteVer = joiningData.ver;

        DistributedMetaStorageHistoryItem[] remoteHist = joiningData.hist;

        int remoteHistSize = remoteHist.length;

        int remoteBltId = joiningData.bltId;

        boolean clusterIsActive = isActive();

        String errorMsg;

        synchronized (innerStateLock) {
            DistributedMetaStorageVersion locVer = getActualVersion();

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
        }

        return (errorMsg == null) ? null : new IgniteNodeValidationResult(node.id(), errorMsg, errorMsg);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData = dataBag.newJoinerDiscoveryData(COMPONENT_ID);

        if (!discoData.hasJoiningNodeData())
            return;

        DistributedMetaStorageJoiningNodeData joiningData =
            (DistributedMetaStorageJoiningNodeData)discoData.joiningNodeData();

        if (joiningData == null)
            return;

        DistributedMetaStorageVersion remoteVer = joiningData.ver;

        synchronized (innerStateLock) {
            //TODO Store it precalculated? Maybe later.
            DistributedMetaStorageVersion actualVer = getActualVersion();

            if (remoteVer.id > actualVer.id) {
                assert startupExtras != null;

                DistributedMetaStorageHistoryItem[] hist = joiningData.hist;

                if (remoteVer.id - actualVer.id <= hist.length) {
                    assert bridge instanceof NotAvailableDistributedMetaStorageBridge
                        || bridge instanceof EmptyDistributedMetaStorageBridge;

                    for (long v = actualVer.id + 1; v <= remoteVer.id; v++)
                        updateLater(hist[(int)(v - remoteVer.id + hist.length - 1)]);

                    Serializable nodeData = new DistributedMetaStorageClusterNodeData(remoteVer, null, null, null);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
                else {
                    assert false : "Joining node is too far ahead [remoteVer=" + remoteVer + "]";
//                    DistributedMetaStorageClusterNodeData nodeData = new DistributedMetaStorageClusterNodeData(
//                        remoteVer,
//                        joiningData.fullData,
//                        joiningData.hist,
//                        EMPTY_ARRAY
//                    );
//
//                    writeFullDataLater(nodeData);
                }
            }
            else {
                if (dataBag.commonDataCollectedFor(COMPONENT_ID))
                    return;

                if (remoteVer.id == actualVer.id) {
                    assert remoteVer.equals(actualVer) : actualVer + " " + remoteVer;

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

                        DistributedMetaStorageHistoryItem[] fullData;

                        DistributedMetaStorageHistoryItem[] hist;

                        if (startupExtras == null || startupExtras.fullNodeData == null) {
                            ver0 = ver;

                            fullData = localFullData(bridge);

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
    }

    /** */
    private int getAvailableHistorySize() {
        assert Thread.holdsLock(innerStateLock);

        if (startupExtras == null)
            return histCache.size();
        else if (startupExtras.fullNodeData == null)
            return histCache.size() + startupExtras.deferredUpdates.size();
        else
            return startupExtras.fullNodeData.hist.length + startupExtras.deferredUpdates.size();
    }

    /** */
    private DistributedMetaStorageVersion getActualVersion() {
        assert Thread.holdsLock(innerStateLock);

        if (startupExtras == null)
            return ver;
        else if (startupExtras.fullNodeData == null)
            return ver.nextVersion(startupExtras.deferredUpdates);
        else
            return startupExtras.fullNodeData.ver.nextVersion(startupExtras.deferredUpdates);
    }

    /** */
    private DistributedMetaStorageHistoryItem historyItem(long specificVer) {
        assert Thread.holdsLock(innerStateLock);

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

    /** */
    private DistributedMetaStorageHistoryItem[] history(long startVer, long actualVer) {
        return LongStream.rangeClosed(startVer, actualVer)
            .mapToObj(this::historyItem)
            .toArray(DistributedMetaStorageHistoryItem[]::new);
    }

    /** */
    @TestOnly
    private DistributedMetaStorageHistoryItem[] localFullData() {
        return localFullData(bridge);
    }

    /** */
    private DistributedMetaStorageHistoryItem[] localFullData(DistributedMetaStorageBridge bridge) {
        if (startupExtras != null && startupExtras.locFullData != null)
            return startupExtras.locFullData;

        List<DistributedMetaStorageHistoryItem> locFullData = new ArrayList<>();

        try {
            bridge.iterate(
                "",
                (key, val) -> locFullData.add(new DistributedMetaStorageHistoryItem(key, (byte[])val)),
                false
            );
        }
        catch (IgniteCheckedException e) {
            //TODO ???
            throw U.convertException(e);
        }

        return locFullData.toArray(EMPTY_ARRAY);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
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
    }

    /** */
    private void startWrite(String key, byte[] valBytes) throws IgniteCheckedException {
        UUID reqId = UUID.randomUUID();

        GridFutureAdapter<Boolean> fut = new DistributedMetaStorageUpdateFuture(reqId, updateFuts);

        DiscoveryCustomMessage msg = new DistributedMetaStorageUpdateMessage(reqId, key, valBytes);

        ctx.discovery().sendCustomEvent(msg);

        fut.get();
    }

    /** */
    private boolean startCas(String key, byte[] expValBytes, byte[] newValBytes) throws IgniteCheckedException {
        UUID reqId = UUID.randomUUID();

        GridFutureAdapter<Boolean> fut = new DistributedMetaStorageUpdateFuture(reqId, updateFuts);

        DiscoveryCustomMessage msg = new DistributedMetaStorageCasMessage(reqId, key, expValBytes, newValBytes);

        ctx.discovery().sendCustomEvent(msg);

        return fut.get();
    }

    /** */
    private void onUpdateMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateMessage msg
    ) {
        if (!isActive()) {
            msg.setActive(false);

            return;
        }

        try {
            U.await(writeAvailable);

            if (msg instanceof DistributedMetaStorageCasMessage)
                completeCas(bridge, (DistributedMetaStorageCasMessage)msg);
            else
                completeWrite(bridge, new DistributedMetaStorageHistoryItem(msg.key(), msg.value()), true);
        }
        catch (IgniteCheckedException | Error e) {
            criticalError(e);
        }
    }

    /** */
    private void onAckMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateAckMessage msg
    ) {
        GridFutureAdapter<Boolean> fut = updateFuts.remove(msg.requestId());

        if (fut != null) {
            if (msg.isActive()) {
                Boolean res = msg instanceof DistributedMetaStorageCasAckMessage
                    ? ((DistributedMetaStorageCasAckMessage)msg).updated()
                    : null;

                fut.onDone(res);
            }
            else
                fut.onDone(clusterIsNotActiveException());
        }
    }

    /** */
    private void criticalError(Throwable e) {
        ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

        if (e instanceof Error)
            throw (Error) e;

        throw U.convertException((IgniteCheckedException)e);
    }

    /** */
    private void completeWrite(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageHistoryItem histItem,
        boolean notifyListeners
    ) throws IgniteCheckedException {
        Serializable val = unmarshal(histItem.valBytes);

        lock();

        try {
            bridge.onUpdateMessage(histItem, val, notifyListeners);

            bridge.write(histItem.key, histItem.valBytes);
        }
        finally {
            unlock();
        }

        addToHistoryCache(ver.id, histItem);

        shrinkHistory(bridge);
    }

    /** */
    private void completeCas(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageCasMessage msg
    ) throws IgniteCheckedException {
        if (!msg.matches())
            return;

        lock();

        try {
            Serializable oldVal = bridge.read(msg.key());

            Serializable expVal = unmarshal(msg.expectedValue());

            if (!Objects.deepEquals(oldVal, expVal)) {
                msg.setMatches(false);

                return;
            }
        }
        finally {
            unlock();
        }

        completeWrite(bridge, new DistributedMetaStorageHistoryItem(msg.key(), msg.value()), true);
    }

    /** */
    void addToHistoryCache(long ver, DistributedMetaStorageHistoryItem histItem) {
        DistributedMetaStorageHistoryItem old = histCache.put(ver, histItem);

        assert old == null : old;

        histSizeApproximation += histItem.estimateSize();
    }

    /** */
    void removeFromHistoryCache(long ver) {
        DistributedMetaStorageHistoryItem old = histCache.remove(ver);

        if (old != null)
            histSizeApproximation -= old.estimateSize();
    }

    /** */
    void clearHistoryCache() {
        histCache.clear();

        histSizeApproximation = 0L;
    }

    /** */
    private void shrinkHistory(
        DistributedMetaStorageBridge bridge
    ) throws IgniteCheckedException {
        long maxBytes = histMaxBytes;

        if (histSizeApproximation > maxBytes && histCache.size() > 1) {
            lock();

            try {
                while (histSizeApproximation > maxBytes && histCache.size() > 1) {
                    bridge.removeHistoryItem(ver.id + 1 - histCache.size());

                    removeFromHistoryCache(ver.id + 1 - histCache.size());
                }
            }
            finally {
                unlock();
            }
        }
    }

    /** */
    private void updateLater(DistributedMetaStorageHistoryItem update) {
        assert startupExtras != null;

        startupExtras.deferredUpdates.add(update);
    }

    /** */
    private void executeDeferredUpdates(DistributedMetaStorageBridge bridge) throws IgniteCheckedException {
        assert startupExtras != null;

        DistributedMetaStorageHistoryItem firstToWrite = startupExtras.firstToWrite;

        if (firstToWrite != null) {
            lock();

            try {
                bridge.write(firstToWrite.key, firstToWrite.valBytes);
            }
            finally {
                unlock();
            }
        }

        for (DistributedMetaStorageHistoryItem histItem : startupExtras.deferredUpdates)
            completeWrite(bridge, histItem, false);

        notifyListenersBeforeReadyForWrite(bridge);
    }

    /** */
    private void notifyListenersBeforeReadyForWrite(
        DistributedMetaStorageBridge bridge
    ) throws IgniteCheckedException {
        DistributedMetaStorageHistoryItem[] oldData = startupExtras.locFullData;

        startupExtras.locFullData = null;

        startupExtras.firstToWrite = null;

        DistributedMetaStorageHistoryItem[] newData = localFullData(bridge);

        int oldIdx = 0, newIdx = 0;

        while (oldIdx < oldData.length && newIdx < newData.length) {
            String oldKey = oldData[oldIdx].key;
            byte[] oldValBytes = oldData[oldIdx].valBytes;

            String newKey = newData[newIdx].key;
            byte[] newValBytes = newData[newIdx].valBytes;

            int c = oldKey.compareTo(newKey);

            if (c < 0) {
                notifyListeners(oldKey, unmarshal(oldValBytes), null);

                ++oldIdx;
            }
            else if (c > 0) {
                notifyListeners(newKey, null, unmarshal(newValBytes));

                ++newIdx;
            }
            else {
                Serializable oldVal = unmarshal(oldValBytes);

                Serializable newVal = Arrays.equals(oldValBytes, newValBytes) ? oldVal : unmarshal(newValBytes);

                notifyListeners(oldKey, oldVal, newVal);

                ++oldIdx;

                ++newIdx;
            }
        }

        for (; oldIdx < oldData.length; ++oldIdx)
            notifyListeners(oldData[oldIdx].key, unmarshal(oldData[oldIdx].valBytes), null);

        for (; newIdx < newData.length; ++newIdx)
            notifyListeners(newData[newIdx].key, null, unmarshal(newData[newIdx].valBytes));
    }

    /** */
    private void calcDiff(
        DistributedMetaStorageHistoryItem[] oldData,
        DistributedMetaStorageHistoryItem[] newData,
        DiffClosure cb
    ) {

    }

    /** */
    private void writeFullDataLater(DistributedMetaStorageClusterNodeData nodeData) {
        assert nodeData.fullData != null;

        startupExtras.clearLocData = true;

        startupExtras.fullNodeData = nodeData;

        startupExtras.firstToWrite = null;

        startupExtras.deferredUpdates.clear();

        if (nodeData.updates != null) {
            for (DistributedMetaStorageHistoryItem update : nodeData.updates)
                updateLater(update);

            nodeData.updates = null;
        }
    }

    /** */
    void notifyListeners(String key, Serializable oldVal, Serializable newVal) {
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

    /** */
    private void lock() {
        ctx.cache().context().database().checkpointReadLock();
    }

    /** */
    private void unlock() {
        ctx.cache().context().database().checkpointReadUnlock();
    }

    /** */
    @FunctionalInterface
    private interface DiffClosure {
        /** */
        void apply(String key, Serializable oldVal, Serializable newVal);
    }
}
