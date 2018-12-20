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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
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
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageListener;
import org.apache.ignite.internal.processors.metastorage.GlobalMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.META_STORAGE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;

/** */
public class DistributedMetaStorageImpl extends GridProcessorAdapter implements DistributedMetaStorage {
    /** */
    private static final int COMPONENT_ID = META_STORAGE.ordinal();

    /** */
    private static final long DFLT_MAX_HISTORY_BYTES = 10 * 1024 * 1024;

    /** */
    private DistributedMetaStorageBridge bridge = new NotAvailableDistributedMetaStorageBridge();

    /** */
    private final CountDownLatch writeAvailable = new CountDownLatch(1);

    /** */
    long ver;

    /** */
    final Set<IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>>> lsnrs =
        new GridConcurrentLinkedHashSet<>();

    /** */
    private final Map<Long, DistributedMetaStorageHistoryItem> histCache = new ConcurrentHashMap<>();

    /** */
    private long histSizeApproximation;

    /** */
    private long histMaxBytes = IgniteSystemProperties.getLong(
        IGNITE_GLOBAL_METASTORAGE_HISTORY_MAX_BYTES,
        DFLT_MAX_HISTORY_BYTES
    );

    /** */
    private final ConcurrentMap<UUID, GridFutureAdapter<?>> updateFuts = new ConcurrentHashMap<>();

    /** */
    private StartupExtras startupExtras = new StartupExtras();

    /**
     * @param ctx Kernal context.
     */
    public DistributedMetaStorageImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

        if (isPersistenceEnabled(ctx.config())) {
            isp.registerMetastorageListener(new MetastorageLifecycleListener() {
                /** {@inheritDoc} */
                @Override public void onReadyForRead(
                    ReadOnlyMetastorage metastorage
                ) throws IgniteCheckedException {
                    onMetaStorageReadyForRead(metastorage, isp);
                }

                /** {@inheritDoc} */
                @Override public void onReadyForReadWrite(
                    ReadWriteMetastorage metastorage
                ) throws IgniteCheckedException {
                    onMetaStorageReadyForWrite(metastorage, isp);
                }
            });
        }
        else {
            bridge = new EmptyDistributedMetaStorageBridge();

            for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
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
        if (!isPersistenceEnabled(ctx.config())) {
            synchronized (this) {
                InMemoryCachedDistributedMetaStorageBridge memCachedBridge =
                    new InMemoryCachedDistributedMetaStorageBridge(this);

                memCachedBridge.restore(startupExtras);

                executeDeferredUpdates(memCachedBridge);

                bridge = memCachedBridge;

                writeAvailable.countDown();

                startupExtras = null;
            }

            GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

            for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
                subscriber.onReadyForWrite(this);
        }
    }

    /** */
    private void onMetaStorageReadyForRead(
        ReadOnlyMetastorage metastorage,
        GridInternalSubscriptionProcessor isp
    ) throws IgniteCheckedException {
        assert isPersistenceEnabled(ctx.config());

        ReadOnlyDistributedMetaStorageBridge readOnlyBridge = new ReadOnlyDistributedMetaStorageBridge(this, metastorage);

        lock();

        try {
            readOnlyBridge.readInitialData(startupExtras);
        }
        finally {
            unlock();
        }

        bridge = readOnlyBridge;

        for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
            subscriber.onReadyForRead(this);
    }

    /** */
    private void onMetaStorageReadyForWrite(
        ReadWriteMetastorage metastorage,
        GridInternalSubscriptionProcessor isp
    ) throws IgniteCheckedException {
        assert isPersistenceEnabled(ctx.config());

        synchronized (this) {
            long metaStorageMaxSize = ctx.config().getDataStorageConfiguration().getSystemRegionMaxSize();

            histMaxBytes = Math.min(histMaxBytes, metaStorageMaxSize / 2);

            WritableDistributedMetaStorageBridge writableBridge = new WritableDistributedMetaStorageBridge(this, metastorage);

            lock();

            try {
                writableBridge.restore(startupExtras);
            }
            finally {
                unlock();
            }

            executeDeferredUpdates(writableBridge);

            bridge = writableBridge;

            startupExtras = null;
        }

        writeAvailable.countDown();

        for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
            subscriber.onReadyForWrite(this);
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

        startWrite(key, JdkMarshaller.DEFAULT.marshal(val));
    }

    /** {@inheritDoc} */
    @Override public void remove(@NotNull String key) throws IgniteCheckedException {
        startWrite(key, null);
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        @NotNull Predicate<String> keyPred,
        @NotNull BiConsumer<String, ? super Serializable> cb
    ) throws IgniteCheckedException {
        lock();

        try {
            bridge.iterate(keyPred, cb, true);
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

        DistributedMetaStorageHistoryItem[] hist = new TreeMap<>(histCache)
            .values()
            .toArray(EMPTY_ARRAY);

        Serializable data = new DistributedMetaStorageJoiningData(startupExtras.verToSnd, hist);

        dataBag.addJoiningNodeData(COMPONENT_ID, data);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        DiscoveryDataBag.JoiningNodeDiscoveryData joiningNodeData = dataBag.newJoinerDiscoveryData(COMPONENT_ID);

        if (joiningNodeData != null && !joiningNodeData.hasJoiningNodeData())
            return;

        if (joiningNodeData == null)
            return;

        DistributedMetaStorageJoiningData joiningData =
            (DistributedMetaStorageJoiningData)joiningNodeData.joiningNodeData();

        if (joiningData == null)
            return;

        long remoteVer = joiningData.ver;

        synchronized (this) {
            long actualVer;

            if (startupExtras == null)
                actualVer = ver;
            else if (startupExtras.fullNodeData == null)
                actualVer = ver + startupExtras.deferredUpdates.size();
            else
                actualVer = startupExtras.fullNodeData.ver + startupExtras.deferredUpdates.size();

            if (remoteVer > actualVer) {
                assert startupExtras != null;

                DistributedMetaStorageHistoryItem[] hist = joiningData.hist;

                if (remoteVer - actualVer <= hist.length) {
                    assert bridge instanceof ReadOnlyDistributedMetaStorageBridge
                        || bridge instanceof EmptyDistributedMetaStorageBridge;

                    for (long v = actualVer + 1; v <= remoteVer; v++)
                        updateLater(hist[(int)(v - remoteVer + hist.length - 1)]);

                    Serializable nodeData = new DistributedMetaStorageNodeData(remoteVer, null, null, null);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
                else {
                    //TODO ???
                }
            }
            else {
                if (dataBag.commonDataCollectedFor(COMPONENT_ID))
                    return;

                if (remoteVer == actualVer) {
                    Serializable nodeData = new DistributedMetaStorageNodeData(ver, null, null, null);

                    dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                }
                else {
                    int availableHistSize;

                    if (startupExtras == null)
                        availableHistSize = histCache.size();
                    else if (startupExtras.fullNodeData == null)
                        availableHistSize = histCache.size() + startupExtras.deferredUpdates.size();
                    else
                        availableHistSize = startupExtras.fullNodeData.hist.length + startupExtras.deferredUpdates.size();

                    if (actualVer - remoteVer <= availableHistSize) {
                        Serializable nodeData = new DistributedMetaStorageNodeData(ver, null, null, history(remoteVer + 1, actualVer));

                        dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                    }
                    else {
                        long ver0;

                        DistributedMetaStorageHistoryItem[] fullData;

                        DistributedMetaStorageHistoryItem[] hist;

                        if (startupExtras == null || startupExtras.fullNodeData == null) {
                            ver0 = ver;

                            fullData = fullData();

                            hist = history(ver - histCache.size() + 1, actualVer);
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

                        Serializable nodeData = new DistributedMetaStorageNodeData(ver0, fullData, hist, updates);

                        dataBag.addGridCommonData(COMPONENT_ID, nodeData);
                    }
                }
            }
        }
    }

    /** */
    private DistributedMetaStorageHistoryItem[] history(long startVer, long actualVer) {
        if (startVer > actualVer)
            return EMPTY_ARRAY;

        List<DistributedMetaStorageHistoryItem> hist = new ArrayList<>((int)(actualVer - startVer + 1));

        if (startupExtras == null) {
            for (long v = startVer; v <= actualVer; v++)
                hist.add(histCache.get(v));
        }
        else {
            DistributedMetaStorageNodeData fullNodeData = startupExtras.fullNodeData;

            if (fullNodeData == null) {
                for (long v = startVer; v <= ver; v++)
                    hist.add(histCache.get(v));
            }
            else {
                long fullNodeDataVer = fullNodeData.ver;

                for (long v = startVer; v <= fullNodeDataVer; v++)
                    hist.add(fullNodeData.hist[(int)(v - fullNodeDataVer + fullNodeData.hist.length - 1)]);
            }

            List<DistributedMetaStorageHistoryItem> deferredUpdates = startupExtras.deferredUpdates;

            int deferredStartVer = (int)(startVer - actualVer + deferredUpdates.size() - 1);

            hist.addAll(deferredUpdates.subList(Math.max(deferredStartVer, 0), deferredUpdates.size()));
        }

        return hist.toArray(EMPTY_ARRAY);
    }

    /** */
    private DistributedMetaStorageHistoryItem[] fullData() {
        List<DistributedMetaStorageHistoryItem> fullData = new ArrayList<>();

        try {
            bridge.iterate(
                key -> true,
                (key, val) -> fullData.add(new DistributedMetaStorageHistoryItem(key, (byte[])val)),
                false
            );
        }
        catch (IgniteCheckedException e) {
            //TODO ???
            throw U.convertException(e);
        }

        return fullData.toArray(EMPTY_ARRAY);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        DistributedMetaStorageNodeData nodeData = (DistributedMetaStorageNodeData)data.commonData();

        if (nodeData.fullData == null) {
            if (nodeData.updates != null) {
                for (DistributedMetaStorageHistoryItem update : nodeData.updates)
                    updateLater(update);
            }
        }
        else
            writeFullDataLater(nodeData);
    }

    /** */
    private void startWrite(String key, byte[] valBytes) throws IgniteCheckedException {
        UUID reqId = UUID.randomUUID();

        GridFutureAdapter<?> fut = new DistributedMetaStorageUpdateFuture(reqId, updateFuts);

        DiscoveryCustomMessage msg = new DistributedMetaStorageUpdateMessage(reqId, key, valBytes);

        ctx.discovery().sendCustomEvent(msg);

        fut.get();
    }

    /** */
    private void onUpdateMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateMessage msg
    ) {
        try {
            U.await(writeAvailable);

            completeWrite(bridge, new DistributedMetaStorageHistoryItem(msg.key(), msg.value()));
        }
        catch (IgniteCheckedException | Error e) {
            criticalError(e);
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
    private void onAckMessage(
        AffinityTopologyVersion topVer,
        ClusterNode node,
        DistributedMetaStorageUpdateAckMessage msg
    ) {
        GridFutureAdapter<?> fut = updateFuts.remove(msg.requestId());

        if (fut != null)
            fut.onDone();
    }

    /** */
    private void completeWrite(
        DistributedMetaStorageBridge bridge,
        DistributedMetaStorageHistoryItem histItem
    ) throws IgniteCheckedException {
        Serializable val = unmarshal(histItem.valBytes);

        lock();

        try {
            bridge.onUpdateMessage(histItem, val);

            bridge.write(histItem.key, histItem.valBytes);
        }
        finally {
            unlock();
        }

        addToHistoryCache(ver, histItem);

        shrinkHistory(bridge);
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
                    bridge.removeHistoryItem(ver + 1 - histCache.size());

                    removeFromHistoryCache(ver + 1 - histCache.size());
                }
            }
            finally {
                unlock();
            }
        }
    }

    /** */
    private void updateLater(DistributedMetaStorageHistoryItem update) {
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
            completeWrite(bridge, histItem);
    }

    /** */
    private void writeFullDataLater(DistributedMetaStorageNodeData nodeData) {
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
    void notifyListeners(String key, Serializable val) {
        for (IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>> entry : lsnrs) {
            if (entry.get1().test(key)) {
                try {
                    // ClassCastException might be thrown here for crappy listeners.
                    entry.get2().onUpdate(key, val);
                }
                catch (Exception e) {
                    log.error(S.toString(
                        "Failed to notify global metastorage update listener",
                        "key", key, false,
                        "val", val, false,
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
}
