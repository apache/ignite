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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.CompactedException;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.KeyValueStorageImpl;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.OperationTimeoutException;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.common.raft.MetaStorageCommandListener;
import org.apache.ignite.network.ClusterNode;
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
@SuppressWarnings("unused") public class MetaStorageManager {
    /** MetaStorage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Cluster network service that is used in order to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    /** Meta storage service. */
    private CompletableFuture<MetaStorageService> metaStorageSvcFut;

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
        ConfigurationManager locCfgMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        watchAggregator = new WatchAggregator();
        deployFut = new CompletableFuture<>();

        String locNodeName = locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .name().value();

        String[] metastorageNodes = locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        Predicate<ClusterNode> metaStorageNodesContainsLocPred =
            clusterNode -> Arrays.asList(metastorageNodes).contains(clusterNode.name());

        if (hasMetastorageLocally(locNodeName, metastorageNodes)) {
            this.metaStorageSvcFut = CompletableFuture.completedFuture(new MetaStorageServiceImpl(
                    raftMgr.startRaftGroup(
                        METASTORAGE_RAFT_GROUP_NAME,
                        clusterNetSvc.topologyService().allMembers().stream().filter(
                            metaStorageNodesContainsLocPred).
                            collect(Collectors.toList()),
                        new MetaStorageCommandListener(new KeyValueStorageImpl())
                    )
                )
            );
        }
        else if (metastorageNodes.length > 0) {
            this.metaStorageSvcFut = CompletableFuture.completedFuture(new MetaStorageServiceImpl(
                    raftMgr.startRaftService(
                        METASTORAGE_RAFT_GROUP_NAME,
                        clusterNetSvc.topologyService().allMembers().stream().filter(
                            metaStorageNodesContainsLocPred).
                            collect(Collectors.toList())
                    )
                )
            );
        }
        else
            this.metaStorageSvcFut = new CompletableFuture<>();

        // TODO: IGNITE-14088: Uncomment and use real serializer factory
//        Arrays.stream(MetaStorageMessageTypes.values()).forEach(
//            msgTypeInstance -> net.registerMessageMapper(
//                msgTypeInstance.msgType(),
//                new DefaultMessageMapperProvider()
//            )
//        );

        // TODO: IGNITE-14414 Cluster initialization flow. Here we should complete metaStorageServiceFuture.
        clusterNetSvc.messagingService().addMessageHandler((message, sender, correlationId) -> {});
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
            metaStorageSvcFut.thenApply(svc -> svc.watch(
                watch.get().keyCriterion().toRange().getKey(),
                watch.get().keyCriterion().toRange().getValue(),
                watch.get().revision(),
                watch.get().listener()).thenAccept(id -> deployFut.complete(Optional.of(id))).join());
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
        return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
    }

    /**
     * @see MetaStorageService#get(Key, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull Key key, long revUpperBound) {
        return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
    }

    /**
     * @see MetaStorageService#getAll(Collection)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
    }

    /**
     * @see MetaStorageService#getAll(Collection, long)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys, long revUpperBound) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
    }

    /**
     * @see MetaStorageService#put(Key, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull Key key, byte[] val) {
        return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
    }

    /**
     * @see MetaStorageService#getAndPut(Key, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull Key key, byte[] val) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndPut(key, val));
    }

    /**
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
    }

    /**
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAndPutAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndPutAll(vals));
    }

    /**
     * @see MetaStorageService#remove(Key)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull Key key) {
        return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
    }

    /**
     * @see MetaStorageService#getAndRemove(Key)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull Key key) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemove(key));
    }

    /**
     * @see MetaStorageService#removeAll(Collection)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<Key> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
    }

    /**
     * @see MetaStorageService#getAndRemoveAll(Collection)
     */
    public @NotNull CompletableFuture<Map<Key, Entry>> getAndRemoveAll(@NotNull Collection<Key> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemoveAll(keys));
    }

    /**
     * Invoke with single success/failure operation.
     *
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
        @NotNull Condition cond,
        @NotNull Operation success,
        @NotNull Operation failure
    ) {
        return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, Collections.singletonList(success), Collections.singletonList(failure)));
    }

    /**
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
        @NotNull Condition cond,
        @NotNull Collection<Operation> success,
        @NotNull Collection<Operation> failure
    ) {
        return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
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
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndInvoke(key, cond, success, failure));
    }

    /**
     * @see MetaStorageService#range(Key, Key, long)
     */
    public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo, long revUpperBound) {
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound))
        );
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see Key
     * @see Entry
     */
    public @NotNull Cursor<Entry> rangeWithAppliedRevision(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> {
                try {
                    return svc.range(keyFrom, keyTo, vaultMgr.appliedRevision());
                }
                catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException(e);
                }
            })
        );
    }

    /**
     * @see MetaStorageService#range(Key, Key)
     */
    public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo))
        );
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
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
            .thenCompose(idOpt -> idOpt.map(id -> metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id))).
                orElse(CompletableFuture.completedFuture(null))
            .thenCompose(r -> {
                var watch = watchAggregator.watch(finalRevision, this::storeEntries);

                if (watch.isEmpty())
                    return CompletableFuture.completedFuture(Optional.empty());
                else
                    return metaStorageSvcFut.thenCompose(svc -> svc.watch(
                        watch.get().keyCriterion().toRange().get1(),
                        watch.get().keyCriterion().toRange().get2(),
                        watch.get().revision(),
                        watch.get().listener()).thenApply(Optional::of));
            }));

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

    /**
     * Checks whether the local node hosts Metastorage.
     *
     * @param locNodeName Local node uniq name.
     * @param metastorageMembers Metastorage members names.
     * @return True if the node has Metastorage, false otherwise.
     */
    public static boolean hasMetastorageLocally(String locNodeName, String[] metastorageMembers) {
        boolean isLocNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(locNodeName)) {
                isLocNodeHasMetasorage = true;

                break;
            }
        }
        return isLocNodeHasMetasorage;
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** MetaStorage service future. */
        private final CompletableFuture<MetaStorageService> metaStorageSvcFut;

        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /** Inner iterator future. */
        private final CompletableFuture<Iterator<T>> innerIterFut;

        /**
         * @param metaStorageSvcFut MetaStorage service future.
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(
            CompletableFuture<MetaStorageService> metaStorageSvcFut,
            CompletableFuture<Cursor<T>> innerCursorFut
        ) {
            this.metaStorageSvcFut = metaStorageSvcFut;
            this.innerCursorFut = innerCursorFut;
            this.innerIterFut = innerCursorFut.thenApply(Iterable::iterator);
        }

            /** {@inheritDoc} */
            @Override public void close() throws Exception {
            innerCursorFut.thenCompose(cursor -> {
                try {
                    cursor.close();

                    return null;
                }
                catch (Exception e) {
                    throw new IgniteInternalException(e);
                }
            }).get();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            return new Iterator<>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    try {
                        return innerIterFut.thenApply(Iterator::hasNext).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new IgniteInternalException(e);
                    }
                }

                /** {@inheritDoc} */
                @Override public T next() {
                    try {
                        return innerIterFut.thenApply(Iterator::next).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new IgniteInternalException(e);
                    }
                }
            };
        }
    }
}
