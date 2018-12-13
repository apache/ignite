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

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.META_STORAGE;

/** */
public class DistributedMetaStorageImpl extends GridProcessorAdapter implements DistributedMetaStorage {
    /** */
    private static final int COMPONENT_ID = META_STORAGE.ordinal();

    /** */
    private static final String GLOBAL_KEY_PREFIX = "\u0000";

    /** */
    private static final String HISTORY_VER_KEY = "ms-hist-ver";

    /** */
    private static final String HISTORY_GUARD_KEY_PREFIX = "ms-hist-grd-";

    /** */
    private static final String HISTORY_ITEM_KEY_PREFIX = "ms-hist-item-";

    /** */
    private static final byte[] HISTORY_GUARD_VALUE = {};

    /** */
    private ReadWriteMetastorage metastorage;

    /** */
    private long ver;

    /** */
    private final Set<IgniteBiTuple<IgnitePredicate<String>, IgniteBiInClosure<String, Serializable>>> lsnrs =
        new GridConcurrentLinkedHashSet<>();

    /** */
    private final Map<Long, HistoryItem> histCache = new ConcurrentHashMap<>();

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

        isp.registerMetastorageListener(new MetastorageLifecycleListener() {
            /** {@inheritDoc} */
            @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
                init(metastorage);

                ReadOnlyDistributedMetaStorage readOnlyImpl = new ReadOnlyDistributedMetaStorageImpl(metastorage);

                for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
                    subscriber.onReadyForRead(readOnlyImpl);
            }

            /** {@inheritDoc} */
            @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
                DistributedMetaStorageImpl.this.metastorage = metastorage;

                restore();

                executeDeferredUpdates();

                startupExtras = null;

                for (GlobalMetastorageLifecycleListener subscriber : isp.getGlobalMetastorageSubscribers())
                    subscriber.onReadyForWrite(DistributedMetaStorageImpl.this);
            }
        });

        ctx.discovery().setCustomEventListener(
            DistributedMetaStorageUpdateMessage.class,
            this::onUpdateMessage
        );

        ctx.discovery().setCustomEventListener(
            DistributedMetaStorageUpdateAckMessage.class,
            this::onAckMessage
        );
    }

    /** */
    public boolean isReady() {
        return startupExtras == null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public <T extends Serializable> T read(@NotNull String key) throws IgniteCheckedException {
        lock();

        try {
            return (T)metastorage.read(localKey(key));
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
        @NotNull IgnitePredicate<String> keyPred,
        @NotNull IgniteBiInClosure<String, ? super Serializable> cb
    ) throws IgniteCheckedException {
        metastorage.iterate(
            key -> isGlobalKey(key) && keyPred.apply(globalKey(key)),
            cb,
            false
        );
    }

    /** {@inheritDoc} */
    @Override public void listen(@NotNull String key, @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr) {
        listen(key::equals, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void listen(
        @NotNull IgnitePredicate<String> keyPred,
        @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr
    ) {
        IgniteBiInClosure<String, Serializable> lsnrUnchecked = (IgniteBiInClosure<String, Serializable>)lsnr;

        lsnrs.add(new IgniteBiTuple<>(keyPred, lsnrUnchecked));
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryDataExchangeType discoveryDataType() {
        return META_STORAGE;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        assert startupExtras != null;

        dataBag.addJoiningNodeData(COMPONENT_ID, startupExtras.verToSnd);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        DiscoveryDataBag.JoiningNodeDiscoveryData joiningNodeData = dataBag.newJoinerDiscoveryData(COMPONENT_ID);

        if (joiningNodeData != null && !joiningNodeData.hasJoiningNodeData())
            return;

        if (joiningNodeData == null || dataBag.commonDataCollectedFor(COMPONENT_ID))
            return;

        Long remoteVer = (Long)joiningNodeData.joiningNodeData();

        assert ver >= remoteVer : "Joining node has larger metastorage version [ver=" + ver + ", remoteVer=" + remoteVer + "]";

        List<HistoryItem> hist = history(remoteVer + 1);

        // "hist" is ArrayList or EmptyList, both are serializable.
        dataBag.addGridCommonData(COMPONENT_ID, (Serializable)hist);

        //TODO Send whole metastorage if required.
    }

    /** */
    private List<HistoryItem> history(long startVer) {
        if (startVer > ver)
            return Collections.emptyList();

        List<HistoryItem> hist = new ArrayList<>((int)(ver - startVer + 1));

        for (long v = startVer; v <= ver; v++)
            hist.add(histCache.get(v));

        return hist;
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        List<HistoryItem> hist = (List<HistoryItem>)data.commonData();

        for (HistoryItem histItem : hist)
            updateLater(histItem);
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
            completeWrite(msg.key(), msg.value());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
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
    private void completeWrite(String key, byte[] valBytes) throws IgniteCheckedException {
        HistoryItem histItem = new HistoryItem(key, valBytes);

        lock();

        try {
            String histGuardKey = historyGuardKey(ver + 1);

            metastorage.write(histGuardKey, HISTORY_GUARD_VALUE);

            metastorage.write(historyItemKey(ver + 1), histItem);

            ++ver;

            metastorage.write(historyVersionKey(), ver);

            Serializable val = valBytes == null ? null : JdkMarshaller.DEFAULT.unmarshal(valBytes, U.gridClassLoader());

            notifyListeners(key, val);

            metastorage.remove(histGuardKey);

            if (valBytes == null)
                metastorage.remove(localKey(key));
            else
                metastorage.putData(localKey(key), valBytes);
        }
        catch (IgniteCheckedException | Error e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            unlock();
        }

        addToHistoryCache(histItem);

        shrinkHistory();
    }

    /** */
    private void init(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        lock();

        try {
            Long storedVer = (Long)metastorage.read(historyVersionKey());

            if (storedVer == null)
                startupExtras.verToSnd = ver = 0;
            else {
                startupExtras.verToSnd = ver = storedVer;

                Serializable guard = metastorage.read(historyGuardKey(ver));

                if (guard != null) {
                    // New value is already known, but listeners may not have been invoked.
                    HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver));

                    assert histItem != null;

                    updateLater(histItem);
                }
                else {
                    guard = metastorage.read(historyGuardKey(ver + 1));

                    if (guard != null) {
                        HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver + 1));

                        if (histItem != null) {
                            ++startupExtras.verToSnd;

                            updateLater(histItem);
                        }
                    }
                    else {
                        HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver));

                        if (histItem != null) {
                            byte[] valBytes = metastorage.getData(localKey(histItem.key));

                            if (!Arrays.equals(valBytes, histItem.valBytes))
                                startupExtras.writeOnRestore = histItem;
                        }
                    }
                }

                metastorage.iterate(
                    DistributedMetaStorageImpl::isHistoryItemKey,
                    (key, val) -> addToHistoryCache((HistoryItem)val),
                    false
                );
            }
        }
        finally {
            unlock();
        }
    }

    /** */
    private void restore() throws IgniteCheckedException {
        assert startupExtras != null;

        lock();

        try {
            Long storedVer = (Long)metastorage.read(historyVersionKey());

            if (storedVer == null)
                metastorage.write(historyVersionKey(), 0L);
            else {
                Serializable guard = metastorage.read(historyGuardKey(ver + 1));

                if (guard != null) {
                    HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver + 1));

                    if (histItem == null)
                        metastorage.remove(historyGuardKey(ver + 1));
                }
            }

            HistoryItem writeOnRestore = startupExtras.writeOnRestore;

            if (writeOnRestore != null) {
                byte[] valBytes = writeOnRestore.valBytes;

                if (valBytes == null)
                    metastorage.remove(localKey(writeOnRestore.key));
                else
                    metastorage.write(localKey(writeOnRestore.key), valBytes);
            }
        }
        finally {
            unlock();
        }
    }

    /** */
    private void addToHistoryCache(HistoryItem histItem) {
        histCache.put(ver, histItem);
    }

    /** */
    private void shrinkHistory() {
        // No-op.
    }

    /** */
    private void updateLater(HistoryItem histItem) {
        startupExtras.deferredUpdates.add(histItem);
    }

    /** */
    private void executeDeferredUpdates() throws IgniteCheckedException {
        assert startupExtras != null;

        for (HistoryItem histItem : startupExtras.deferredUpdates)
            completeWrite(histItem.key, histItem.valBytes);
    }

    /** */
    private void notifyListeners(String key, Serializable val) {
        for (IgniteBiTuple<IgnitePredicate<String>, IgniteBiInClosure<String, Serializable>> entry : lsnrs) {
            if (entry.get1().apply(key)) {
                try {
                    // ClassCastException might be thrown here for crappy listeners.
                    entry.get2().apply(key, val);
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
        return GLOBAL_KEY_PREFIX + globalKey;
    }

    /** */
    private static String globalKey(String locKey) {
        assert locKey.startsWith(GLOBAL_KEY_PREFIX) : locKey;

        return locKey.substring(GLOBAL_KEY_PREFIX.length());
    }

    /** */
    private static boolean isGlobalKey(String key) {
        return key.startsWith(GLOBAL_KEY_PREFIX);
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
    private static String historyVersionKey() {
        return GLOBAL_KEY_PREFIX + HISTORY_VER_KEY;
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
    private static class HistoryItem implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public final String key;

        /** */
        public final byte[] valBytes;

        /** */
        public HistoryItem(String key, byte[] valBytes) {
            this.key = key;
            this.valBytes = valBytes;
        }
    }

    /** */
    @SuppressWarnings("PublicField")
    private static class StartupExtras {
        /** */
        public long verToSnd;

        /** */
        public List<HistoryItem> deferredUpdates = new ArrayList<>();

        /** */
        public HistoryItem writeOnRestore;
    }

    /** */
    private class ReadOnlyDistributedMetaStorageImpl implements ReadOnlyDistributedMetaStorage {
        /** */
        private final ReadOnlyMetastorage metastorage;

        /** */
        public ReadOnlyDistributedMetaStorageImpl(ReadOnlyMetastorage metastorage) {
            this.metastorage = metastorage;
        }

        /** {@inheritDoc} */
        @Override public <T extends Serializable>
        @Nullable T read(@NotNull String key) throws IgniteCheckedException {
            lock();

            try {
                return (T)metastorage.read(localKey(key));
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void iterate(
            @NotNull IgnitePredicate<String> keyPred,
            @NotNull IgniteBiInClosure<String, ? super Serializable> cb
        ) throws IgniteCheckedException {
            metastorage.iterate(
                key -> isGlobalKey(key) && keyPred.apply(globalKey(key)),
                cb,
                false
            );
        }

        /** {@inheritDoc} */
        @Override public void listen(
            @NotNull String key,
            @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr
        ) {
            DistributedMetaStorageImpl.this.listen(key, lsnr);
        }

        /** {@inheritDoc} */
        @Override public void listen(
            @NotNull IgnitePredicate<String> keyPred,
            @NotNull IgniteBiInClosure<String, ? extends Serializable> lsnr
        ) {
            DistributedMetaStorageImpl.this.listen(keyPred, lsnr);
        }
    }
}
