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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
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
    private static final ClassLoader CLASS_LOADER = DistributedMetaStorageImpl.class.getClassLoader();

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
    private MetaStorage metastorage;

    /** */
    private long ver;

    /** */
    private long verToSnd;

    /** */
    private final Map<IgnitePredicate<String>, IgniteBiInClosure<String, Serializable>> lsnrs =
        new ConcurrentHashMap<>();

    /** */
    private final Map<Long, HistoryItem> histCache = new ConcurrentHashMap<>();

    /** */
    private List<HistoryItem> deferredUpdates = new ArrayList<>();

    /** */
    private final ConcurrentMap<UUID, GridFutureAdapter<?>> updateFuts = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public DistributedMetaStorageImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(new MetastorageLifecycleListener() {
            /** {@inheritDoc} */
            @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
                init(metastorage);
            }

            /** {@inheritDoc} */
            @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
                DistributedMetaStorageImpl.this.metastorage = ctx.cache().context().database().metaStorage();

                restore();

                executeDeferredUpdates();
            }
        });

        ctx.discovery().setCustomEventListener(
            DistributedMetaStorageUpdateMessage.class,
            this::onUpdateMessage
        );
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> @Nullable T read(@NotNull String key) throws IgniteCheckedException {
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
        @NotNull IgniteInClosure<String> cb
    ) throws IgniteCheckedException {
        metastorage.iterate(
            key -> isGlobalKey(key) && keyPred.apply(globalKey(key)),
            (key, val) -> cb.apply(key),
            true
        );
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

        lsnrs.put(keyPred, lsnrUnchecked);
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryDataExchangeType discoveryDataType() {
        return META_STORAGE;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(COMPONENT_ID, verToSnd);
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
        if (!msg.isAckMessage()) {
            try {
                completeWrite(msg.key(), msg.value());
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        if (msg.isAckMessage()) {
            GridFutureAdapter<?> fut = updateFuts.remove(msg.requestId());

            if (fut != null)
                fut.onDone();
        }
    }

    /** */
    private void completeWrite(String key, byte[] valBytes) throws IgniteCheckedException {
        HistoryItem histItem = new HistoryItem(key, valBytes);

        lock();

        try {
            String histGuardKey = historyGuardKey(ver + 1);

            metastorage.write(histGuardKey, HISTORY_GUARD_VALUE);

            metastorage.write(historyItemKey(ver + 1), histItem);

            if (valBytes == null)
                metastorage.remove(localKey(key));
            else
                metastorage.putData(localKey(key), valBytes);

            ++ver;

            metastorage.write(historyVersionKey(), ver);

            notifyListeners(key, valBytes);

            metastorage.remove(histGuardKey);
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
                verToSnd = ver = 0;
            else {
                verToSnd = ver = storedVer;

                Serializable guard = metastorage.read(historyGuardKey(ver));

                if (guard != null) {
                    // New ver has already been written into metastorage. Transaction should be completed.
                    HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver));

                    updateLater(histItem);
                }
                else {
                    guard = metastorage.read(historyGuardKey(ver + 1));

                    if (guard != null) {
                        HistoryItem histItem = (HistoryItem)metastorage.read(historyItemKey(ver + 1));

                        if (histItem != null) {
                            ++verToSnd;

                            updateLater(histItem);
                        }
                    }
                }

                Map<String, ? extends Serializable> storedHist =
                    metastorage.readForPredicate(key -> key.startsWith(GLOBAL_KEY_PREFIX + HISTORY_ITEM_KEY_PREFIX));

                for (Serializable histItem : storedHist.values())
                    addToHistoryCache((HistoryItem)histItem);
            }
        }
        finally {
            unlock();
        }
    }

    /** */
    private void restore() throws IgniteCheckedException {
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
        deferredUpdates.add(histItem);
    }

    /** */
    private void executeDeferredUpdates() throws IgniteCheckedException {
        for (HistoryItem histItem : deferredUpdates)
            completeWrite(histItem.key, histItem.valBytes);

        deferredUpdates = null;
    }

    /** */
    private void notifyListeners(String key, byte[] valBytes) throws IgniteCheckedException {
        Serializable val = valBytes == null ? null : JdkMarshaller.DEFAULT.unmarshal(valBytes, CLASS_LOADER);

        for (Map.Entry<IgnitePredicate<String>, IgniteBiInClosure<String, Serializable>> entry : lsnrs.entrySet()) {
            if (entry.getKey().apply(key)) {
                // ClassCastException might be thrown here for crappy listeners.
                entry.getValue().apply(key, val);
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
}
