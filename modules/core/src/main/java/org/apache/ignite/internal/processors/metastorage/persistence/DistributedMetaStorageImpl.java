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
import java.util.HashSet;
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

/** */
public class DistributedMetaStorageImpl extends GridProcessorAdapter implements DistributedMetaStorage {
    /** */
    private static final int COMPONENT_ID = META_STORAGE.ordinal();

    /** */
    private static final long DFLT_MAX_HISTORY_BYTES = 10 * 1024 * 1024;

    /** */
    private static final String GLOBAL_KEY_PREFIX = "\u0000";

    /** */
    private static final String KEY_PREFIX = "key-";

    /** */
    private static final String HISTORY_VER_KEY = "hist-ver";

    /** */
    private static final String HISTORY_GUARD_KEY_PREFIX = "hist-grd-";

    /** */
    private static final String HISTORY_ITEM_KEY_PREFIX = "hist-item-";

    /** */
    private static final String CLEANUP_KEY = "cleanup";

    /** */
    private static final byte[] DUMMY_VALUE = {};

    /** */
    private DistributedMetaStorageBridge bridge = new NotAvailableDistributedMetaStorageBridge();

    /** */
    private final CountDownLatch writeAvailable = new CountDownLatch(1);

    /** */
    private long ver;

    /** */
    private final Set<IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>>> lsnrs =
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
            InMemoryCachedDistributedMetaStorageBridge memCachedBridge =
                new InMemoryCachedDistributedMetaStorageBridge();

            memCachedBridge.restore();

            executeDeferredUpdates(memCachedBridge);

            bridge = memCachedBridge;

            writeAvailable.countDown();

            startupExtras = null;

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

        ReadOnlyDistributedMetaStorageBridge readOnlyBridge = new ReadOnlyDistributedMetaStorageBridge(metastorage);

        lock();

        try {
            readOnlyBridge.readInitialData();
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

            WritableDistributedMetaStorageBridge writableBridge = new WritableDistributedMetaStorageBridge(metastorage);

            lock();

            try {
                writableBridge.restore();
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

        GridFutureAdapter<?> fut = new DistributedMetaStorageUpdateFuture(reqId);

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
    @Nullable private static Serializable unmarshal(byte[] valBytes) throws IgniteCheckedException {
        return valBytes == null ? null : JdkMarshaller.DEFAULT.unmarshal(valBytes, U.gridClassLoader());
    }

    /** */
    private void addToHistoryCache(long ver, DistributedMetaStorageHistoryItem histItem) {
        histCache.put(ver, histItem);

        histSizeApproximation += histItem.estimateSize();
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

                    DistributedMetaStorageHistoryItem histItem = histCache.remove(ver + 1 - histCache.size());

                    histSizeApproximation -= histItem.estimateSize();
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
    private void clearLocalDataLater() {
        startupExtras.clearLocData = true;
    }

    /** */
    private void writeFullDataLater(DistributedMetaStorageNodeData nodeData) {
        assert nodeData.fullData != null;

        clearLocalDataLater();

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
    private void notifyListeners(String key, Serializable val) {
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
    private static String localKey(String globalKey) {
        return GLOBAL_KEY_PREFIX + KEY_PREFIX + globalKey;
    }

    /** */
    private static String globalKey(String locKey) {
        assert isLocalKey(locKey) : locKey;

        return locKey.substring((GLOBAL_KEY_PREFIX + KEY_PREFIX).length());
    }

    /** */
    private static boolean isLocalKey(String key) {
        return key.startsWith(GLOBAL_KEY_PREFIX + KEY_PREFIX);
    }

    /** */
    private static String historyGuardKey(long ver) {
        return GLOBAL_KEY_PREFIX + HISTORY_GUARD_KEY_PREFIX + ver;
    }

    /** */
    private static String historyItemKey(long ver) {
        return GLOBAL_KEY_PREFIX + HISTORY_ITEM_KEY_PREFIX + ver;
    }

    /** */
    private static boolean isHistoryItemKey(String locKey) {
        return locKey.startsWith(GLOBAL_KEY_PREFIX + HISTORY_ITEM_KEY_PREFIX);
    }

    /** */
    private static long historyItemVer(String histItemKey) {
        assert isHistoryItemKey(histItemKey);

        return Long.parseLong(histItemKey.substring((GLOBAL_KEY_PREFIX + HISTORY_ITEM_KEY_PREFIX).length()));
    }

    /** */
    private static String historyVersionKey() {
        return GLOBAL_KEY_PREFIX + HISTORY_VER_KEY;
    }

    /** */
    private static String cleanupKey() {
        return GLOBAL_KEY_PREFIX + CLEANUP_KEY;
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
    private class DistributedMetaStorageUpdateFuture extends GridFutureAdapter<Void> {
        /** */
        private final UUID id;

        /** */
        public DistributedMetaStorageUpdateFuture(UUID id) {
            this.id = id;

            updateFuts.put(id, this);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            updateFuts.remove(id);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DistributedMetaStorageUpdateFuture.class, this);
        }
    }

    /** */
    @SuppressWarnings("PublicField")
    private static class StartupExtras {
        /** */
        public long verToSnd;

        /** */
        public List<DistributedMetaStorageHistoryItem> deferredUpdates = new ArrayList<>();

        /** */
        public DistributedMetaStorageHistoryItem firstToWrite;

        /** */
        public DistributedMetaStorageNodeData fullNodeData;

        /** */
        public boolean clearLocData;
    }

    /** */
    private interface DistributedMetaStorageBridge {
        /** */
        Serializable read(String globalKey) throws IgniteCheckedException;

        /** */
        void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) throws IgniteCheckedException;

        /** */
        void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException;

        /** */
        void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) throws IgniteCheckedException;

        /** */
        void removeHistoryItem(long ver) throws IgniteCheckedException;
    }

    /** */
    private static class NotAvailableDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
        /** {@inheritDoc} */
        @Override public Serializable read(String globalKey) {
            throw new UnsupportedOperationException("read");
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) {
            throw new UnsupportedOperationException("iterate");
        }

        /** {@inheritDoc} */
        @Override public void write(String globalKey, byte[] valBytes) {
            throw new UnsupportedOperationException("write");
        }

        /** {@inheritDoc} */
        @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) {
            throw new UnsupportedOperationException("onUpdateMessage");
        }

        /** {@inheritDoc} */
        @Override public void removeHistoryItem(long ver) {
            throw new UnsupportedOperationException("removeHistoryItem");
        }
    }

    /** */
    private static class EmptyDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
        /** {@inheritDoc} */
        @Override public Serializable read(String globalKey) {
            throw new UnsupportedOperationException("read");
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) {
            throw new UnsupportedOperationException("iterate");
        }

        /** {@inheritDoc} */
        @Override public void write(String globalKey, byte[] valBytes) {
            throw new UnsupportedOperationException("write");
        }

        /** {@inheritDoc} */
        @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) {
            throw new UnsupportedOperationException("onUpdateMessage");
        }

        /** {@inheritDoc} */
        @Override public void removeHistoryItem(long ver) {
            throw new UnsupportedOperationException("removeHistoryItem");
        }
    }

    /** */
    private class ReadOnlyDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
        /** */
        private final ReadOnlyMetastorage metastorage;

        /** */
        public ReadOnlyDistributedMetaStorageBridge(ReadOnlyMetastorage metastorage) {
            this.metastorage = metastorage;
        }

        /** {@inheritDoc} */
        @Override public Serializable read(String globalKey) throws IgniteCheckedException {
            return metastorage.read(localKey(globalKey));
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) throws IgniteCheckedException {
            metastorage.iterate(
                key -> isLocalKey(key) && globalKeyPred.test(globalKey(key)),
                cb,
                unmarshal
            );
        }

        /** {@inheritDoc} */
        @Override public void write(String globalKey, byte[] valBytes) {
            throw new UnsupportedOperationException("write");
        }

        /** {@inheritDoc} */
        @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) {
            throw new UnsupportedOperationException("onUpdateMessage");
        }

        /** {@inheritDoc} */
        @Override public void removeHistoryItem(long ver) {
            throw new UnsupportedOperationException("removeHistoryItem");
        }

        /** */
        public void readInitialData() throws IgniteCheckedException {
            String cleanupKey = cleanupKey();
            if (metastorage.getData(cleanupKey) != null) {
                clearLocalDataLater();

                startupExtras.verToSnd = ver = 0;
            }
            else {
                Long storedVer = (Long)metastorage.read(historyVersionKey());

                if (storedVer == null)
                    startupExtras.verToSnd = ver = 0;
                else {
                    startupExtras.verToSnd = ver = storedVer;

                    Serializable guard = metastorage.read(historyGuardKey(ver));

                    if (guard != null) {
                        // New value is already known, but listeners may not have been invoked.
                        DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(ver));

                        assert histItem != null;

                        updateLater(histItem);
                    }
                    else {
                        guard = metastorage.read(historyGuardKey(ver + 1));

                        if (guard != null) {
                            DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(ver + 1));

                            if (histItem != null) {
                                ++startupExtras.verToSnd;

                                updateLater(histItem);
                            }
                        }
                        else {
                            DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(ver));

                            if (histItem != null) {
                                byte[] valBytes = metastorage.getData(localKey(histItem.key));

                                if (!Arrays.equals(valBytes, histItem.valBytes))
                                    startupExtras.firstToWrite = histItem;
                            }
                        }
                    }

                    metastorage.iterate(
                        DistributedMetaStorageImpl::isHistoryItemKey,
                        (key, val) -> addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                        true
                    );
                }
            }
        }
    }

    /** */
    private class WritableDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
        /** */
        private final ReadWriteMetastorage metastorage;

        /** */
        public WritableDistributedMetaStorageBridge(ReadWriteMetastorage metastorage) {
            this.metastorage = metastorage;
        }

        /** {@inheritDoc} */
        @Override public Serializable read(String globalKey) throws IgniteCheckedException {
            return metastorage.read(localKey(globalKey));
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) throws IgniteCheckedException {
            metastorage.iterate(
                key -> isLocalKey(key) && globalKeyPred.test(globalKey(key)),
                cb,
                unmarshal
            );
        }

        /** {@inheritDoc} */
        @Override public void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException {
            if (valBytes == null)
                metastorage.remove(localKey(globalKey));
            else
                metastorage.putData(localKey(globalKey), valBytes);
        }

        /** {@inheritDoc} */
        @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) throws IgniteCheckedException {
            String histGuardKey = historyGuardKey(ver + 1);

            metastorage.write(histGuardKey, DUMMY_VALUE);

            metastorage.write(historyItemKey(ver + 1), histItem);

            ++ver;

            metastorage.write(historyVersionKey(), ver);

            notifyListeners(histItem.key, val);

            metastorage.remove(histGuardKey);
        }

        /** {@inheritDoc} */
        @Override public void removeHistoryItem(long ver) throws IgniteCheckedException {
            metastorage.remove(historyItemKey(ver));
        }

        /** */
        private void restore() throws IgniteCheckedException {
            assert startupExtras != null;

            if (startupExtras.clearLocData || startupExtras.fullNodeData != null) {
                String cleanupKey = cleanupKey();

                metastorage.putData(cleanupKey, DUMMY_VALUE);

                if (startupExtras.clearLocData) {
                    Set<String> allKeys = new HashSet<>();

                    metastorage.iterate(key -> key.startsWith(GLOBAL_KEY_PREFIX), (key, val) -> allKeys.add(key), false);

                    for (String key : allKeys)
                        metastorage.remove(key);
                }

                if (startupExtras.fullNodeData != null) {
                    DistributedMetaStorageNodeData fullNodeData = startupExtras.fullNodeData;

                    ver = fullNodeData.ver;

                    histCache.clear();

                    histSizeApproximation = 0L;

                    for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData)
                        metastorage.putData(item.key, item.valBytes);

                    for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                        DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                        long histItemVer = ver + i + 1 - len;

                        metastorage.write(historyItemKey(histItemVer), histItem);

                        addToHistoryCache(histItemVer, histItem);
                    }

                    metastorage.write(historyVersionKey(), ver);

                    for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData) {
                        Serializable val = unmarshal(item.valBytes);

                        for (IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>> entry : lsnrs) {
                            if (entry.get1().test(item.key))
                                entry.get2().onInit(item.key, val);
                        }
                    }
                }

                metastorage.remove(cleanupKey);
            }

            Long storedVer = (Long)metastorage.read(historyVersionKey());

            if (storedVer == null)
                metastorage.write(historyVersionKey(), 0L);
            else {
                Serializable guard = metastorage.read(historyGuardKey(ver + 1));

                if (guard != null) {
                    DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(ver + 1));

                    if (histItem == null)
                        metastorage.remove(historyGuardKey(ver + 1));
                }
            }
        }
    }

    /** */
    private class InMemoryCachedDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
        /** */
        private final Map<String, byte[]> cache = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Serializable read(String globalKey) throws IgniteCheckedException {
            byte[] valBytes = cache.get(globalKey);

            return unmarshal(valBytes);
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            Predicate<String> globalKeyPred,
            BiConsumer<String, ? super Serializable> cb,
            boolean unmarshal
        ) throws IgniteCheckedException {
            for (Map.Entry<String, byte[]> entry : cache.entrySet()) {
                if (globalKeyPred.test(entry.getKey()))
                    cb.accept(entry.getKey(), unmarshal ? unmarshal(entry.getValue()) : entry.getValue());
            }
        }

        /** {@inheritDoc} */
        @Override public void write(String globalKey, @Nullable byte[] valBytes) {
            if (valBytes == null)
                cache.remove(globalKey);
            else
                cache.put(globalKey, valBytes);
        }

        /** {@inheritDoc} */
        @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) {
            ++ver;

            notifyListeners(histItem.key, val);
        }

        /** {@inheritDoc} */
        @Override public void removeHistoryItem(long ver) {
        }

        /** */
        public void restore() throws IgniteCheckedException {
            if (startupExtras.fullNodeData != null) {
                DistributedMetaStorageNodeData fullNodeData = startupExtras.fullNodeData;

                assert startupExtras.firstToWrite == null;

                assert startupExtras.deferredUpdates.isEmpty();

                ver = fullNodeData.ver;

                for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData)
                    cache.put(item.key, item.valBytes);

                for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                    DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                    addToHistoryCache(ver + i + 1 - len, histItem);
                }

                for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData) {
                    Serializable val = unmarshal(item.valBytes);

                    for (IgniteBiTuple<Predicate<String>, DistributedMetaStorageListener<Serializable>> entry : lsnrs) {
                        if (entry.get1().test(item.key))
                            entry.get2().onInit(item.key, val);
                    }
                }
            }
        }
    }
}
