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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.client.CompactedException;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.MetaStorageService;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.KeyCriterion;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;

/**
 * MetaStorage manager is responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing meta storage lifecycle including instantiation meta storage raft group.</li>
 *     <li>Providing corresponding meta storage service proxy interface</li>
 * </ul>
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings("unused")
public class MetaStorageManager implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageManager.class);

    /** Meta storage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /**
     * Special key for the vault where the applied revision for {@link MetaStorageManager#storeEntries}
     * operation is stored. This mechanism is needed for committing processed watches to {@link VaultManager}.
     */
    public static final ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Configuration manager that handles local configuration. */
    private final ConfigurationManager locCfgMgr;

    /** Cluster network service that is used in order to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    /** Meta storage service. */
    private volatile CompletableFuture<MetaStorageService> metaStorageSvcFut;

    /** Raft group service. */
    private volatile CompletableFuture<RaftGroupService> raftGroupServiceFut;

    /**
     * Aggregator of multiple watches to deploy them as one batch.
     *
     * @see WatchAggregator
     */
    private final WatchAggregator watchAggregator = new WatchAggregator();

    /**
     * Future which will be completed with {@link IgniteUuid},
     * when aggregated watch will be successfully deployed.
     * Can be resolved to {@link Optional#empty()} if no watch deployed at the moment.
     */
    private CompletableFuture<Optional<IgniteUuid>> deployFut = new CompletableFuture<>();

    /**
     * If true - all new watches will be deployed immediately.
     *
     * If false - all new watches will be aggregated to one batch
     * for further deploy by {@link MetaStorageManager#deployWatches()}
     */
    private boolean deployed;

    /** Flag indicates if meta storage nodes were set on start */
    private boolean metaStorageNodesOnStart;

    /** Actual storage for the Metastorage. */
    private final KeyValueStorage storage;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param locCfgMgr Local configuration manager.
     * @param clusterNetSvc Cluster network service.
     * @param raftMgr Raft manager.
     * @param storage Storage. This component owns this resource and will manage its lifecycle.
     */
    public MetaStorageManager(
        VaultManager vaultMgr,
        ConfigurationManager locCfgMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr,
        KeyValueStorage storage
    ) {
        this.vaultMgr = vaultMgr;
        this.locCfgMgr = locCfgMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        this.storage = storage;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        String[] metastorageNodes = this.locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        Predicate<ClusterNode> metaStorageNodesContainsLocPred =
            clusterNode -> Arrays.asList(metastorageNodes).contains(clusterNode.name());

        if (metastorageNodes.length > 0) {
            metaStorageNodesOnStart = true;

            List<ClusterNode> metaStorageMembers = clusterNetSvc.topologyService().allMembers().stream()
                .filter(metaStorageNodesContainsLocPred)
                .collect(Collectors.toList());

            // TODO: This is temporary solution for providing human-readable error when you try to start single-node cluster
            // without hosting metastorage, this will be rewritten in init phase https://issues.apache.org/jira/browse/IGNITE-14414
            if (metaStorageMembers.isEmpty())
                throw new IgniteException(
                    "Cannot start meta storage manager because there is no node in the cluster that hosts meta storage.");

            storage.start();

            raftGroupServiceFut = raftMgr.prepareRaftGroup(
                METASTORAGE_RAFT_GROUP_NAME,
                metaStorageMembers,
                () -> new MetaStorageListener(storage)
            );

            this.metaStorageSvcFut = raftGroupServiceFut.thenApply(service ->
                new MetaStorageServiceImpl(service, clusterNetSvc.topologyService().localMember().id())
            );

            if (hasMetastorageLocally(locCfgMgr)) {
                clusterNetSvc.topologyService().addEventHandler(new TopologyEventHandler() {
                    @Override public void onAppeared(ClusterNode member) {
                        // No-op.
                    }

                    @Override public void onDisappeared(ClusterNode member) {
                        metaStorageSvcFut.thenCompose(svc -> svc.closeCursors(member.id()));
                    }
                });
            }
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
//        clusterNetSvc.messagingService().addMessageHandler((message, senderAddr, correlationId) -> {});
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        busyLock.block();

        Optional<IgniteUuid> watchId;

        try {
            // If deployed future is not done, that means that stop was called in the middle of
            // IgniteImpl.start, before deployWatches, or before init phase.
            // It is correct to check completeness of the future because the method calls are guarded by busy lock.
            // TODO: add busy lock for init method https://issues.apache.org/jira/browse/IGNITE-14414
            if (deployFut.isDone()) {
                watchId = deployFut.get();

                try {
                    if (watchId.isPresent())
                        metaStorageSvcFut.get().stopWatch(watchId.get());
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to get meta storage service.");

                    throw new IgniteInternalException(e);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to get watch.");

            throw new IgniteInternalException(e);
        }

        try {
            if (raftGroupServiceFut != null) {
                raftGroupServiceFut.get().shutdown();

                raftMgr.stopRaftGroup(METASTORAGE_RAFT_GROUP_NAME, metastorageNodes());
            }
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to get meta storage raft group service.");

            throw new IgniteInternalException(e);
        }

        try {
            storage.close();
        }
        catch (Exception e) {
            throw new IgniteInternalException("Exception when stopping the storage", e);
        }
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            var watch = watchAggregator.watch(
                appliedRevision() + 1,
                this::storeEntries
            );

            if (watch.isEmpty())
                deployFut.complete(Optional.empty());
            else {
                CompletableFuture<Void> fut =
                    dispatchAppropriateMetaStorageWatch(watch.get()).thenAccept(id -> deployFut.complete(Optional.of(id)));

                if (metaStorageNodesOnStart)
                    fut.join();
                else {
                    // TODO: need to wait for this future in init phase https://issues.apache.org/jira/browse/IGNITE-14414
                }
            }

            deployed = true;
        } finally {
            busyLock.leaveBusy();
        }
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
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return waitForReDeploy(watchAggregator.add(key, lsnr));
        } finally {
            busyLock.leaveBusy();
        }
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
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return waitForReDeploy(watchAggregator.addPrefix(key, lsnr));
        }
        finally {
            busyLock.leaveBusy();
        }
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
        @NotNull Collection<ByteArray> keys,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return waitForReDeploy(watchAggregator.add(keys, lsnr));
        }
        finally {
            busyLock.leaveBusy();
        }
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
        @NotNull ByteArray from,
        @NotNull ByteArray to,
        @NotNull WatchListener lsnr
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return waitForReDeploy(watchAggregator.add(from, to, lsnr));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Unregister watch listener by id.
     *
     * @param id of watch to unregister.
     * @return future, which will be completed when unregister finished.
     */
    public synchronized CompletableFuture<Void> unregisterWatch(long id) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            watchAggregator.cancel(id);
            if (deployed)
                return updateWatches().thenAccept(v -> {});
            else
                return deployFut.thenAccept(uuid -> {});
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#get(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#get(ByteArray, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAll(Set, long)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#put(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndPut(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, byte[] val) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPut(key, val));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndPutAll(vals));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#remove(ByteArray)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndRemove(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemove(key));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#removeAll(Set)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#getAndRemoveAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemoveAll(keys));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Invoke with single success/failure operation.
     *
     * @see MetaStorageService#invoke(Condition, Operation, Operation)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
        @NotNull Condition cond,
        @NotNull Operation success,
        @NotNull Operation failure
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition cond,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray, long)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
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
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> rangeWithAppliedRevision(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, appliedRevision()))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * Prefix query is a synonym of the range query {@code (prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefixWithAppliedRevision(@NotNull ByteArray keyPrefix) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);

            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), appliedRevision()))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Short cut for
     * {@link #prefix(ByteArray, long)} where {@code revUpperBound == -1}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix) throws NodeStoppingException {
        return prefix(keyPrefix, -1);
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound
     * of given revision number.
     *
     * Prefix query is a synonym of the range query {@code range(prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound  The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix, long revUpperBound) throws NodeStoppingException {
        if (!busyLock.enterBusy())
            throw new NodeStoppingException();

        try {
            var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);
            return new CursorWrapper<>(
                metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), revUpperBound))
            );
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        if (!busyLock.enterBusy())
            return CompletableFuture.failedFuture(new NodeStoppingException());

        try {
            return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @return Applied revision for {@link VaultManager#putAll} operation.
     */
    private long appliedRevision() {
        byte[] appliedRevision = vaultMgr.get(APPLIED_REV).join().value();

        return appliedRevision == null ? 0L : bytesToLong(appliedRevision);
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<Optional<IgniteUuid>> updateWatches() {
        long revision = appliedRevision() + 1;

        deployFut = deployFut
            .thenCompose(idOpt ->
                idOpt
                    .map(id -> metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id)))
                    .orElseGet(() -> CompletableFuture.completedFuture(null))
            )
            .thenCompose(r ->
                watchAggregator.watch(revision, this::storeEntries)
                    .map(watch -> dispatchAppropriateMetaStorageWatch(watch).thenApply(Optional::of))
                    .orElseGet(() -> CompletableFuture.completedFuture(Optional.empty()))
            );

        return deployFut;
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     */
    private void storeEntries(Collection<IgniteBiTuple<ByteArray, byte[]>> entries, long revision) {
        Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(entries.size() + 1);

        batch.put(APPLIED_REV, longToBytes(revision));

        entries.forEach(e -> batch.put(e.getKey(), e.getValue()));

        byte[] appliedRevisionBytes = vaultMgr.get(APPLIED_REV).join().value();

        long appliedRevision = appliedRevisionBytes == null ? 0L : bytesToLong(appliedRevisionBytes);

        if (revision <= appliedRevision) {
            throw new IgniteInternalException(String.format(
                "Current revision (%d) must be greater than the revision in the Vault (%d)",
                revision, appliedRevision
            ));
        }

        vaultMgr.putAll(batch).join();
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
     * Checks whether the given node hosts meta storage.
     *
     * @param nodeName Node unique name.
     * @param metastorageMembers Meta storage members names.
     * @return {@code true} if the node has meta storage, {@code false} otherwise.
     */
    public static boolean hasMetastorage(String nodeName, String[] metastorageMembers) {
        boolean isNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(nodeName)) {
                isNodeHasMetasorage = true;

                break;
            }
        }

        return isNodeHasMetasorage;
    }

    /**
     * Checks whether the local node hosts meta storage.
     *
     * @param configurationMgr Configuration manager.
     * @return {@code true} if the node has meta storage, {@code false} otherwise.
     */
    public boolean hasMetastorageLocally(ConfigurationManager configurationMgr) {
        String[] metastorageMembers = configurationMgr
            .configurationRegistry()
            .getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes()
            .value();

        return hasMetastorage(vaultMgr.name().join(), metastorageMembers);
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /** Inner iterator future. */
        private final CompletableFuture<Iterator<T>> innerIterFut;

        private final InnerIterator it = new InnerIterator();

        /**
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(
            CompletableFuture<Cursor<T>> innerCursorFut
        ) {
            this.innerCursorFut = innerCursorFut;
            this.innerIterFut = innerCursorFut.thenApply(Iterable::iterator);
        }

            /** {@inheritDoc} */
        @Override public void close() throws Exception {
            if (!busyLock.enterBusy())
                throw new NodeStoppingException();

            try {
                innerCursorFut.thenApply(cursor -> {
                    try {
                        cursor.close();

                        return null;
                    }
                    catch (Exception e) {
                        throw new IgniteInternalException(e);
                    }
                }).get();
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            return it;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
            return it.next();
        }

        private class InnerIterator implements Iterator<T> {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (!busyLock.enterBusy())
                    return false;

                try {
                    try {
                        return innerIterFut.thenApply(Iterator::hasNext).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new IgniteInternalException(e);
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }

            /** {@inheritDoc} */
            @Override public T next() {
                if (!busyLock.enterBusy())
                    throw new NoSuchElementException("No such element because node is stopping.");

                try {
                    try {
                        return innerIterFut.thenApply(Iterator::next).get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new IgniteInternalException(e);
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        }
    }

    /**
     * Dispatches appropriate metastorage watch method according to inferred watch criterion.
     *
     * @param aggregatedWatch Aggregated watch.
     * @return Future, which will be completed after new watch registration finished.
     */
    private CompletableFuture<IgniteUuid> dispatchAppropriateMetaStorageWatch(AggregatedWatch aggregatedWatch) {
        if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.CollectionCriterion) {
            var criterion = (KeyCriterion.CollectionCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.keys(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.ExactCriterion) {
            var criterion = (KeyCriterion.ExactCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.key(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.RangeCriterion) {
            var criterion = (KeyCriterion.RangeCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.from(),
                criterion.to(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else
            throw new UnsupportedOperationException("Unsupported type of criterion");
    }

    /**
     * Return metastorage nodes.
     *
     * This code will be deleted after node init phase is developed.
     * https://issues.apache.org/jira/browse/IGNITE-14414
     */
    private List<ClusterNode> metastorageNodes() {
        String[] metastorageNodes = this.locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        Predicate<ClusterNode> metaStorageNodesContainsLocPred =
            clusterNode -> Arrays.asList(metastorageNodes).contains(clusterNode.name());

        List<ClusterNode> metaStorageMembers = clusterNetSvc.topologyService().allMembers().stream()
            .filter(metaStorageNodesContainsLocPred)
            .collect(Collectors.toList());

        return metaStorageMembers;
    }
}
