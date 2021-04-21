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

import java.util.concurrent.CompletableFuture;

import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.internal.raft.Loza;
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
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterNetSvc Cluster network service.
     * @param raftMgr Raft manager.
     */
    public MetaStorageManager(
        VaultManager vaultMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;

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
     * Register subscription on meta storage updates for further deployment when DMS is ready.
     *
     * @param key The target key. Couldn't be {@code null}.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription.
     */
    public synchronized CompletableFuture<Long> registerWatch(@Nullable Key key, @NotNull WatchListener lsnr) {
        // TODO: IGNITE-14446 Implement DMS manager with watch registry.
        return null;
    }

    /**
     * Unregister subscription for the given identifier.
     *
     * @param id Subscription identifier.
     * @return Completed future in case of operation success. Couldn't be {@code null}.
     */
    public synchronized CompletableFuture<Void> unregisterWatch(long id) {
        // TODO: IGNITE-14446 Implement DMS manager with watch registry.
        return null;
    }

    /**
     * Deploy all registered watches through{@code MetaStorageService.watch()}.
     */
    public synchronized void deployWatches() {
        // TODO: IGNITE-14446 Implement DMS manager with watch registry.
    }
}
