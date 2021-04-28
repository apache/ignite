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

package org.apache.ignite.internal.metastorage;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * MetaStorage manager is responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing meta storage lifecycle including instantiation meta storage raft group.</li>
 *     <li>Providing corresponding meta storage service proxy interface</li>
 * </ul>
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused", "WeakerAccess"}) public class MetaStorageManager {
    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Cluster network service that is used in order to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    /** Meta storage service. */
    private MetaStorageService metaStorageSvc;

    /**
     * Aggregator of multiple watches to deploy them as one batch.
     *
     * @see WatchAggregator
     */
    private final WatchAggregator watchAggregator;

    /**
     * Future which will be completed with {@link IgniteUuid},
     * when aggregated watch will be successfully deployed.
     * Can be resolved to {@link Optional#empty()} if no watch deployed at the moment.
     */
    private CompletableFuture<Optional<IgniteUuid>> deployFut;

    /**
     * If true - all new watches will be deployed immediately.
     *
     * If false - all new watches will be aggregated to one batch
     * for further deploy by {@link MetaStorageManager#deployWatches()}
     */
    private boolean deployed;

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterNetSvc Cluster network service.
     * @param raftMgr Raft manager.
     */
    public MetaStorageManager(
        VaultManager vaultMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr,
        MetaStorageService metaStorageSvc
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        this.metaStorageSvc = metaStorageSvc;
        watchAggregator = new WatchAggregator();
        deployFut = new CompletableFuture<>();

        // TODO: IGNITE-14088: Uncomment and use real serializer factory
//        Arrays.stream(MetaStorageMessageTypes.values()).forEach(
//            msgTypeInstance -> net.registerMessageMapper(
//                msgTypeInstance.msgType(),
//                new DefaultMessageMapperProvider()
//            )
//        );

        clusterNetSvc.messagingService().addMessageHandler((message, sender, correlationId) -> {
            // TODO: IGNITE-14414 Cluster initialization flow.
        });
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() {
        try {
            var watch = watchAggregator.watch(
                vaultMgr.appliedRevision(),
                this::storeEntries
            );
        if (watch.isEmpty())
            deployFut.complete(Optional.empty());
        else
            metaStorageSvc.watch(
                watch.get().keyCriterion().toRange().getKey(),
                watch.get().keyCriterion().toRange().getValue(),
                watch.get().revision(),
                watch.get().listener()).thenAccept(id -> deployFut.complete(Optional.of(id))).join();
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't receive applied revision during deploy watches", e);
        }

        deployed = true;
    }

    /**
     * Register watch listener by key.
     *
     * @param key The target key.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @Nullable Key key,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(key, lsnr));
    }

    /**
     * Register watch listener by key prefix.
     *
     * @param key Prefix to listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatchByPrefix(
        @Nullable Key key,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.addPrefix(key, lsnr));
    }

    /**
     * Register watch listener by collection of keys.
     *
     * @param keys Collection listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @NotNull Collection<Key> keys,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(keys, lsnr));
    }

    /**
     * Register watch listener by range of keys.
     *
     * @param from Start key of range.
     * @param to End key of range (exclusively).
     * @param lsnr Listener which will be notified for each update.
     * @return future with id of registered watch.
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @NotNull Key from,
        @NotNull Key to,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(from, to, lsnr));
    }

    /**
     * Unregister watch listener by id.
     *
     * @param id of watch to unregister.
     * @return future, which will be completed when unregister finished.
     */
    public synchronized CompletableFuture<Void> unregisterWatch(long id) {
        watchAggregator.cancel(id);
        if (deployed)
            return updateWatches().thenAccept(v -> {});
        else
            return deployFut.thenAccept(uuid -> {});
    }

    /**
     * @see MetaStorageService#get(Key)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull Key key) {
        return metaStorageSvc.get(key);
    }

    /**
     * @see MetaStorageService#get(Key, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull Key key, long revUpperBound) {
        return metaStorageSvc.get(key, revUpperBound);
    }

    /**
     * @see MetaStorageService#getAll(Collection)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys) {
        return metaStorageSvc.getAll(keys);
    }

    /**
     * @see MetaStorageService#getAll(Collection, long)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys, long revUpperBound) {
        return metaStorageSvc.getAll(keys, revUpperBound);
    }

    /**
     * @see MetaStorageService#put(Key, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull Key key, byte[] val) {
        return metaStorageSvc.put(key, val);
    }

    /**
     * @see MetaStorageService#getAndPut(Key, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull Key key, byte[] val) {
        return metaStorageSvc.getAndPut(key, val);
    }

    /**
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageSvc.putAll(vals);
    }

    /**
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAndPutAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageSvc.getAndPutAll(vals);
    }

    /**
     * @see MetaStorageService#remove(Key)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull Key key) {
        return metaStorageSvc.remove(key);
    }

    /**
     * @see MetaStorageService#getAndRemove(Key)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull Key key) {
        return metaStorageSvc.getAndRemove(key);
    }

    /**
     * @see MetaStorageService#removeAll(Collection)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<Key> keys) {
        return metaStorageSvc.removeAll(keys);
    }

    /**
     * @see MetaStorageService#getAndRemoveAll(Collection)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAndRemoveAll(@NotNull Collection<Key> keys) {
        return metaStorageSvc.getAndRemoveAll(keys);
    }

    /**
     * @see MetaStorageService#invoke(Key, Condition, Operation, Operation)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
        @NotNull Key key,
        @NotNull Condition cond,
        @NotNull Operation success,
        @NotNull Operation failure
    ) {
        return metaStorageSvc.invoke(key, cond, success, failure);
    }

    /**
     * @see MetaStorageService#getAndInvoke(Key, Condition, Operation, Operation)
     */
    public @NotNull CompletableFuture<Entry> getAndInvoke(
        @NotNull Key key,
        @NotNull Condition cond,
        @NotNull Operation success,
        @NotNull Operation failure
    ) {
        return metaStorageSvc.getAndInvoke(key, cond, success, failure);
    }

    /**
     * @see MetaStorageService#range(Key, Key, long)
     */
    public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo, long revUpperBound) {
        return metaStorageSvc.range(keyFrom, keyTo, revUpperBound);
    }

    /**
     * @see MetaStorageService#range(Key, Key)
     */
    public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return metaStorageSvc.range(keyFrom, keyTo);
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        return metaStorageSvc.compact();
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<Optional<IgniteUuid>> updateWatches() {
        Long revision;
        try {
            revision = vaultMgr.appliedRevision();
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't receive applied revision during watch redeploy", e);
        }

        final var finalRevision = revision;

        deployFut = deployFut
            .thenCompose(idOpt -> idOpt.map(metaStorageSvc::stopWatch).orElse(CompletableFuture.completedFuture(null)))
            .thenCompose(r -> {
                var watch = watchAggregator.watch(finalRevision, this::storeEntries);

                if (watch.isEmpty())
                    return CompletableFuture.completedFuture(Optional.empty());
                else
                    return metaStorageSvc.watch(
                        watch.get().keyCriterion().toRange().get1(),
                        watch.get().keyCriterion().toRange().get2(),
                        watch.get().revision(),
                        watch.get().listener()).thenApply(Optional::of);
            });

        return deployFut;
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     * @return future, which will be completed when store action finished.
     */
    private CompletableFuture<Void> storeEntries(Collection<IgniteBiTuple<Key, byte[]>> entries, long revision) {
        try {
            return vaultMgr.putAll(entries.stream().collect(
                Collectors.toMap(
                    e -> ByteArray.fromString(e.getKey().toString()),
                    IgniteBiTuple::getValue)),
                revision);
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't put entries with considered revision.", e);
        }
    }

    /**
     * @param id of watch to redeploy.
     * @return future, which will be completed after redeploy finished.
     */
    private CompletableFuture<Long> waitForReDeploy(long id) {
        if (deployed)
            return updateWatches().thenApply(uid -> id);
        else
            return deployFut.thenApply(uid -> id);
    }
}
