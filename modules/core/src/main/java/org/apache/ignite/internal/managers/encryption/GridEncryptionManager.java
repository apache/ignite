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

package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.ReencryptionStartRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ENCRYPTION_MGR;
import static org.apache.ignite.internal.GridTopic.TOPIC_GEN_ENC_KEY;
import static org.apache.ignite.internal.IgniteFeatures.CACHE_GROUP_KEY_CHANGE;
import static org.apache.ignite.internal.IgniteFeatures.MASTER_KEY_CHANGE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.MASTER_KEY_CHANGE_FINISH;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.MASTER_KEY_CHANGE_PREPARE;

/**
 * Manages cache keys and {@code EncryptionSpi} instances.
 *
 * NOTE: Following protocol applied to statically configured caches.
 * For dynamically created caches key generated in request creation.
 *
 * Group keys generation protocol:
 *
 * <ul>
 *     <li>Joining node:
 *     <ul>
 *         <li>1. Collects and send all stored group keys to coordinator.</li>
 *         <li>2. Generate(but doesn't store locally!)
 *         and send keys for all statically configured groups in case the not presented in metastore.</li>
 *         <li>3. Store all keys received from coordinator to local store.</li>
 *     </ul>
 *     </li>
 *     <li>Coordinator:
 *     <ul>
 *         <li>1. Checks master key digest are equal to local. If not join is rejected.</li>
 *         <li>2. Checks all stored keys from joining node are equal to stored keys. If not join is rejected.</li>
 *         <li>3. Collects all stored keys and sends it to joining node.</li>
 *     </ul>
 *     </li>
 *     <li>All nodes:
 *     <ul>
 *         <li>1. If new key for group doesn't exists locally it added to local store.</li>
 *         <li>2. If new key for group exists locally, then received key skipped.</li>
 *         <li>3. If a cache group is encrypted with a different (previous) encryption key, then background
 *                re-encryption of this group with a new key is started.</li>
 *     </ul>
 *     </li>
 * </ul>
 * <p>Master key change process:</p>
 * <ol>
 *     <li>The initiator starts the process.</li>
 *     <li>Each server node compares the master key digest. If not equals - the process finishes with error.</li>
 *     <li>Each server node changes the master key: creates WAL record and re-encrypts group keys in MetaStore.</li>
 *     <li>The initiator gets the result when all server nodes completed the master key change.</li>
 * </ol>
 *
 * @see #prepareMKChangeProc
 * @see #performMKChangeProc
 */
public class GridEncryptionManager extends GridManagerAdapter<EncryptionSpi> implements EncryptionCacheKeyProvider,
    MetastorageLifecycleListener, IgniteChangeGlobalStateSupport, IgniteEncryption, PartitionsExchangeAware {
    /**
     * Cache encryption introduced in this Ignite version.
     */
    private static final IgniteProductVersion CACHE_ENCRYPTION_SINCE = IgniteProductVersion.fromString("2.7.0");

    /** Prefix for a master key name. */
    public static final String MASTER_KEY_NAME_PREFIX = "encryption-master-key-name";

    /** Prefix for a encryption group key in meta store, which contains encryption keys with identifiers. */
    public static final String ENCRYPTION_KEYS_PREFIX = "grp-encryption-keys-";

    /** Initial identifier for cache group encryption key. */
    public static final int INITIAL_KEY_ID = 0;

    /** The name on the meta store key, that contains wal segments encrypted using previous encryption keys. */
    private static final String REENCRYPTED_WAL_SEGMENTS = "reencrypted-wal-segments";

    /** Prefix for a encryption group key in meta store. */
    @Deprecated
    private static final String ENCRYPTION_KEY_PREFIX = "grp-encryption-key-";

    /** Synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Synchronization mutex for generate encryption keys and change master key operations. */
    private final Object opsMux = new Object();

    /** Master key change lock. The main purpose is to avoid encrypt and decrypt group keys when master key changing. */
    private final ReentrantReadWriteLock masterKeyChangeLock = new ReentrantReadWriteLock();

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /** Stopped flag. */
    private volatile boolean stopped;

    /** Flag to enable/disable write to metastore on cluster state change. */
    private volatile boolean writeToMetaStoreEnabled;

    /** Cache group encryption keys. */
    private CacheGroupEncryptionKeys grpKeys;

    /** Pending generate encryption key futures. */
    private ConcurrentMap<IgniteUuid, GenerateEncryptionKeyFuture> genEncKeyFuts = new ConcurrentHashMap<>();

    /** Metastorage. */
    private volatile ReadWriteMetastorage metaStorage;

    /** I/O message listener. */
    private GridMessageListener ioLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** {@code True} if the master key name restored from WAL. */
    private volatile boolean restoredFromWAL;

    /** {@code True} if the master key name recovered before startup. */
    private volatile boolean recoveryMasterKeyName;

    /** Master key change future. Not {@code null} on request initiator. */
    private KeyChangeFuture masterKeyChangeFut;

    /** Pending master key request or {@code null} if there is no ongoing master key change process. */
    private volatile MasterKeyChangeRequest masterKeyChangeRequest;

    /** Digest of last changed master key or {@code null} if master key was not changed. */
    private volatile byte[] masterKeyDigest;

    /**
     * Master key change prepare process. Checks that all server nodes have the same new master key and then starts
     * finish process.
     */
    private DistributedProcess<MasterKeyChangeRequest, EmptyResult> prepareMKChangeProc;

    /** Process to perform the master key change. Changes master key and reencrypt group keys. */
    private DistributedProcess<MasterKeyChangeRequest, EmptyResult> performMKChangeProc;

    /**
     * A two-phase distributed process that rotates the encryption keys of specified cache groups and initiates
     * re-encryption of those cache groups.
     */
    private GroupKeyChangeProcess grpKeyChangeProc;

    /** Cache groups for which encryption key was changed, and they must be re-encrypted. */
    private final Map<Integer, long[]> reencryptGroups = new ConcurrentHashMap<>();

    /** Cache groups for which encryption key was changed on node join. */
    private final Map<Integer, Integer> reencryptGroupsForced = new ConcurrentHashMap<>();

    /** Cache group page stores scanner. */
    private CacheGroupPageScanner pageScanner;

    /**
     * @param ctx Kernel context.
     */
    public GridEncryptionManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getEncryptionSpi());

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            UUID leftNodeId = evt.eventNode().id();

            synchronized (opsMux) {
                Iterator<Map.Entry<IgniteUuid, GenerateEncryptionKeyFuture>> futsIter =
                    genEncKeyFuts.entrySet().iterator();

                while (futsIter.hasNext()) {
                    GenerateEncryptionKeyFuture fut = futsIter.next().getValue();

                    if (!F.eq(leftNodeId, fut.nodeId()))
                        return;

                    try {
                        futsIter.remove();

                        sendGenerateEncryptionKeyRequest(fut);

                        genEncKeyFuts.put(fut.id(), fut);
                    }
                    catch (IgniteCheckedException e) {
                        fut.onDone(null, e);
                    }
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctx.io().addMessageListener(TOPIC_GEN_ENC_KEY, ioLsnr = (nodeId, msg, plc) -> {
            synchronized (opsMux) {
                if (msg instanceof GenerateEncryptionKeyRequest) {
                    GenerateEncryptionKeyRequest req = (GenerateEncryptionKeyRequest)msg;

                    assert req.keyCount() != 0;

                    List<byte[]> encKeys = new ArrayList<>(req.keyCount());

                    byte[] masterKeyDigest = withMasterKeyChangeReadLock(() -> {
                        for (int i = 0; i < req.keyCount(); i++)
                            encKeys.add(getSpi().encryptKey(getSpi().create()));

                        // We should send the master key digest that encrypted group keys because the response can be
                        // processed after the possible master key change.
                        return getSpi().masterKeyDigest();
                    });

                    try {
                        ctx.io().sendToGridTopic(nodeId, TOPIC_GEN_ENC_KEY,
                            new GenerateEncryptionKeyResponse(req.id(), encKeys, masterKeyDigest), SYSTEM_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Unable to send generate key response[nodeId=" + nodeId + "]");
                    }
                }
                else {
                    GenerateEncryptionKeyResponse resp = (GenerateEncryptionKeyResponse)msg;

                    GenerateEncryptionKeyFuture fut = genEncKeyFuts.get(resp.requestId());

                    if (fut != null)
                        fut.onDone(new T2<>(resp.encryptionKeys(), resp.masterKeyDigest()), null);
                    else
                        U.warn(log, "Response received for a unknown request.[reqId=" + resp.requestId() + "]");
                }
            }
        });

        prepareMKChangeProc = new DistributedProcess<>(ctx, MASTER_KEY_CHANGE_PREPARE, this::prepareMasterKeyChange,
            this::finishPrepareMasterKeyChange);

        performMKChangeProc = new DistributedProcess<>(ctx, MASTER_KEY_CHANGE_FINISH, this::performMasterKeyChange,
            this::finishPerformMasterKeyChange);

        grpKeys = new CacheGroupEncryptionKeys(getSpi());
        pageScanner = new CacheGroupPageScanner(ctx);
        grpKeyChangeProc = new GroupKeyChangeProcess(ctx, grpKeys);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        pageScanner.stop();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() {
        ctx.cache().context().exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        synchronized (opsMux) {
            stopped = true;

            if (ioLsnr != null)
                ctx.io().removeMessageListener(TOPIC_GEN_ENC_KEY, ioLsnr);

            if (discoLsnr != null)
                ctx.event().removeDiscoveryEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

            cancelFutures("Kernal stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        synchronized (opsMux) {
            assert !disconnected;

            disconnected = true;

            masterKeyChangeRequest = null;

            masterKeyDigest = null;

            cancelFutures("Client node was disconnected from topology (operation result is unknown).");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) {
        synchronized (opsMux) {
            assert disconnected;

            disconnected = false;

            return null;
        }
    }

    /**
     * Callback for local join.
     */
    public void onLocalJoin() {
        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return;

        //We can't store keys before node join to cluster(on statically configured cache registration).
        //Because, keys should be received from cluster.
        //Otherwise, we would generate different keys on each started node.
        //So, after starting, coordinator saves locally newly generated encryption keys.
        //And sends that keys to every joining node.
        synchronized (metaStorageMux) {
            //Keys read from meta storage.
            HashMap<Integer, GroupKeyEncrypted> knownEncKeys = grpKeys.getAll();

            //Generated(not saved!) keys for a new caches.
            //Configured statically in config, but doesn't stored on the disk.
            HashMap<Integer, byte[]> newEncKeys =
                newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

            if (newEncKeys == null)
                return;

            //We can store keys to the disk, because we are on a coordinator.
            for (Map.Entry<Integer, byte[]> entry : newEncKeys.entrySet()) {
                addGroupKey(entry.getKey(), new GroupKeyEncrypted(INITIAL_KEY_ID, entry.getValue()));

                U.quietAndInfo(log, "Added encryption key on local join [grpId=" + entry.getKey() + "]");
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node,
        JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = super.validateNode(node, discoData);

        if (res != null)
            return res;

        if (isMasterKeyChangeInProgress()) {
            // Prevents new nodes join to avoid inconsistency of the master key.
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Master key change is in progress! Node join is rejected. [node=" + node.id() + "]",
                "Master key change is in progress! Node join is rejected.");
        }

        if (node.isClient() || node.isDaemon())
            return null;

        res = validateNode(node);

        if (res != null)
            return res;

        if (grpKeyChangeProc.inProgress()) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Cache group key change is in progress! Node join is rejected. [node=" + node.id() + "]",
                "Cache group key change is in progress! Node join is rejected.");
        }

        NodeEncryptionKeys nodeEncKeys = (NodeEncryptionKeys)discoData.joiningNodeData();

        if (!discoData.hasJoiningNodeData() || nodeEncKeys == null) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Joining node doesn't have encryption data [node=" + node.id() + "]",
                "Joining node doesn't have encryption data.");
        }

        if (!Arrays.equals(getSpi().masterKeyDigest(), nodeEncKeys.masterKeyDigest)) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Master key digest differs! Node join is rejected. [node=" + node.id() + "]",
                "Master key digest differs! Node join is rejected.");
        }

        if (!IgniteFeatures.nodeSupports(node, CACHE_GROUP_KEY_CHANGE)) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Joining node doesn't support multiple encryption keys for single group [node=" + node.id() + "]",
                "Joining node doesn't support multiple encryption keys for single group.");
        }

        if (F.isEmpty(nodeEncKeys.knownKeys)) {
            U.quietAndInfo(log, "Joining node doesn't have stored group keys [node=" + node.id() + "]");

            return null;
        }

        assert !F.isEmpty(nodeEncKeys.knownKeysWithIds);

        for (Map.Entry<Integer, List<GroupKeyEncrypted>> entry : nodeEncKeys.knownKeysWithIds.entrySet()) {
            int grpId = entry.getKey();

            GroupKey locEncKey = getActiveKey(grpId);

            if (locEncKey == null)
                continue;

            List<GroupKeyEncrypted> rmtKeys = entry.getValue();

            if (rmtKeys == null)
                continue;

            GroupKeyEncrypted rmtKeyEncrypted = null;

            for (GroupKeyEncrypted rmtKey0 : rmtKeys) {
                if (rmtKey0.id() != locEncKey.unsignedId())
                    continue;

                rmtKeyEncrypted = rmtKey0;

                break;
            }

            if (rmtKeyEncrypted == null || F.eq(locEncKey.key(), getSpi().decryptKey(rmtKeyEncrypted.key())))
                continue;

            // The remote node should not rotate the cache key to the current one
            // until the old key (with an identifier that is currently active in the cluster) is removed.
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Cache key differs! Node join is rejected. [node=" + node.id() + ", grp=" + entry.getKey() + "]",
                "Cache key differs! Node join is rejected.");
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        if (ctx.clientNode())
            return;

        Set<Integer> grpIds = grpKeys.groupIds();

        HashMap<Integer, List<GroupKeyEncrypted>> knownEncKeys = U.newHashMap(grpIds.size());

        for (int grpId : grpIds)
            knownEncKeys.put(grpId, grpKeys.getAll(grpId));

        HashMap<Integer, byte[]> newKeys = newEncryptionKeys(grpIds);

        if (log.isInfoEnabled()) {
            String knownGrps = F.isEmpty(knownEncKeys) ? null : F.concat(knownEncKeys.keySet(), ",");

            if (knownGrps != null)
                U.quietAndInfo(log, "Sending stored group keys to coordinator [grps=" + knownGrps + "]");

            String newGrps = F.isEmpty(newKeys) ? null : F.concat(newKeys.keySet(), ",");

            if (newGrps != null)
                U.quietAndInfo(log, "Sending new group keys to coordinator [grps=" + newGrps + "]");
        }

        dataBag.addJoiningNodeData(ENCRYPTION_MGR.ordinal(),
            new NodeEncryptionKeys(knownEncKeys, newKeys, getSpi().masterKeyDigest()));
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData data) {
        NodeEncryptionKeys nodeEncryptionKeys = (NodeEncryptionKeys)data.joiningNodeData();

        if (nodeEncryptionKeys == null || nodeEncryptionKeys.newKeys == null || ctx.clientNode())
            return;

        for (Map.Entry<Integer, byte[]> entry : nodeEncryptionKeys.newKeys.entrySet()) {
            if (getActiveKey(entry.getKey()) == null) {
                U.quietAndInfo(log, "Store group key received from joining node [node=" +
                    data.joiningNodeId() + ", grp=" + entry.getKey() + "]");

                addGroupKey(entry.getKey(), new GroupKeyEncrypted(INITIAL_KEY_ID, entry.getValue()));
            }
            else {
                U.quietAndInfo(log, "Skip group key received from joining node. Already exists. [node=" +
                    data.joiningNodeId() + ", grp=" + entry.getKey() + "]");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (dataBag.isJoiningNodeClient() || dataBag.commonDataCollectedFor(ENCRYPTION_MGR.ordinal()))
            return;

        HashMap<Integer, GroupKeyEncrypted> knownEncKeys = grpKeys.getAll();

        HashMap<Integer, byte[]> newKeys =
            newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

        if (!F.isEmpty(newKeys)) {
            if (knownEncKeys == null)
                knownEncKeys = new HashMap<>();

            for (Map.Entry<Integer, byte[]> entry : newKeys.entrySet()) {
                GroupKeyEncrypted old =
                    knownEncKeys.putIfAbsent(entry.getKey(), new GroupKeyEncrypted(INITIAL_KEY_ID, entry.getValue()));

                assert old == null;
            }
        }

        dataBag.addGridCommonData(ENCRYPTION_MGR.ordinal(), knownEncKeys);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        if (ctx.clientNode())
            return;

        Map<Integer, Object> encKeysFromCluster = (Map<Integer, Object>)data.commonData();

        if (F.isEmpty(encKeysFromCluster))
            return;

        for (Map.Entry<Integer, Object> entry : encKeysFromCluster.entrySet()) {
            int grpId = entry.getKey();

            GroupKeyEncrypted rmtKey;

            if (entry.getValue() instanceof GroupKeyEncrypted)
                rmtKey = (GroupKeyEncrypted)entry.getValue();
            else
                rmtKey = new GroupKeyEncrypted(INITIAL_KEY_ID, (byte[])entry.getValue());

            GroupKey locGrpKey = getActiveKey(grpId);

            if (locGrpKey != null && locGrpKey.unsignedId() == rmtKey.id()) {
                U.quietAndInfo(log, "Skip group key received from coordinator. Already exists. [grp=" +
                    grpId + ", keyId=" + rmtKey.id() + "]");

                continue;
            }

            U.quietAndInfo(log, "Store group key received from coordinator [grp=" + grpId +
                ", keyId=" + rmtKey.id() + "]");

            grpKeys.addKey(grpId, rmtKey);

            if (locGrpKey == null)
                continue;

            GroupKey prevKey = grpKeys.changeActiveKey(grpId, rmtKey.id());

            if (ctx.config().getDataStorageConfiguration().getWalMode() != WALMode.NONE)
                grpKeys.reserveWalKey(grpId, prevKey.unsignedId(), ctx.cache().context().wal().currentSegment());

            reencryptGroupsForced.put(grpId, rmtKey.id());
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public GroupKey getActiveKey(int grpId) {
        return grpKeys.getActiveKey(grpId);
    }

    /** {@inheritDoc} */
    @Override @Nullable public GroupKey groupKey(int grpId, int keyId) {
        return grpKeys.getKey(grpId, keyId);
    }

    /**
     * Gets the existing encryption key IDs for the specified cache group.
     *
     * @param grpId Cache group ID.
     * @return List of the key identifiers.
     */
    @Nullable public List<Integer> groupKeyIds(int grpId) {
        return grpKeys.keyIds(grpId);
    }

    /**
     * Adds new cache group encryption key.
     *
     * @param grpId Cache group ID.
     * @param key Encryption key.
     */
    void addGroupKey(int grpId, GroupKeyEncrypted key) {
        synchronized (metaStorageMux) {
            try {
                grpKeys.addKey(grpId, key);

                writeGroupKeysToMetaStore(grpId, grpKeys.getAll(grpId));
            } catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to write cache group encryption key [grpId=" + grpId + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> changeMasterKey(String masterKeyName) {
        if (ctx.clientNode()) {
            return new IgniteFinishedFutureImpl<>(new UnsupportedOperationException("Client and daemon nodes can not " +
                "perform this operation."));
        }

        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), MASTER_KEY_CHANGE)) {
            return new IgniteFinishedFutureImpl<>(new IllegalStateException("Not all nodes in the cluster support " +
                "the master key change process."));
        }

        // WAL is unavailable for write on the inactive cluster. Master key change will not be logged and group keys
        // can be partially re-encrypted in case of node stop without the possibility of recovery.
        if (!ctx.state().clusterState().active()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                "The cluster is inactive."));
        }

        if (masterKeyName.equals(getMasterKeyName())) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                "New name equal to the current."));
        }

        byte[] digest;

        try {
            digest = masterKeyDigest(masterKeyName);
        } catch (Exception e) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                "Unable to get the master key digest.", e));
        }

        MasterKeyChangeRequest request = new MasterKeyChangeRequest(UUID.randomUUID(), encryptKeyName(masterKeyName),
            digest);

        synchronized (opsMux) {
            if (disconnected) {
                return new IgniteFinishedFutureImpl<>(new IgniteClientDisconnectedException(
                    ctx.cluster().clientReconnectFuture(),
                    "Master key change was rejected. Client node disconnected."));
            }

            if (stopped) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                    "Node is stopping."));
            }

            if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone()) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                    "The previous change was not completed."));
            }

            masterKeyChangeFut = new KeyChangeFuture(request.requestId());

            prepareMKChangeProc.start(request.requestId(), request);

            return new IgniteFutureImpl<>(masterKeyChangeFut);
        }
    }

    /** {@inheritDoc} */
    @Override public String getMasterKeyName() {
        if (ctx.clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        return withMasterKeyChangeReadLock(() -> getSpi().getMasterKeyName());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> changeCacheGroupKey(Collection<String> cacheOrGrpNames) {
        A.notEmpty(cacheOrGrpNames, "cacheOrGrpNames");

        synchronized (opsMux) {
            if (stopped) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Cache group key change was rejected. " +
                    "Node is stopping."));
            }

            return grpKeyChangeProc.start(cacheOrGrpNames);
        }
    }

    /**
     * @param grpIds Cache group IDs.
     * @param keyIds Encryption key IDs.
     * @param keys Encryption keys.
     * @throws IgniteCheckedException If failed.
     */
    protected void changeCacheGroupKeyLocal(int[] grpIds, byte[] keyIds, byte[][] keys) throws IgniteCheckedException {
        Map<Integer, Byte> encryptionStatus = U.newHashMap(grpIds.length);

        for (int i = 0; i < grpIds.length; i++)
            encryptionStatus.put(grpIds[i], keyIds[i]);

        WALPointer ptr = ctx.cache().context().wal().log(new ReencryptionStartRecord(encryptionStatus));

        if (ptr != null)
            ctx.cache().context().wal().flush(ptr, false);

        for (int i = 0; i < grpIds.length; i++) {
            int grpId = grpIds[i];
            int newKeyId = keyIds[i] & 0xff;

            withMasterKeyChangeReadLock(() -> {
                synchronized (metaStorageMux) {
                    // Set new key as key for writing. Note that we cannot pass the encrypted key here because the master
                    // key may have changed in which case we will not be able to decrypt the cache encryption key.
                    GroupKey prevGrpKey = grpKeys.changeActiveKey(grpId, newKeyId);

                    writeGroupKeysToMetaStore(grpId, grpKeys.getAll(grpId));

                    if (ptr == null)
                        return null;

                    grpKeys.reserveWalKey(grpId, prevGrpKey.unsignedId(), ctx.cache().context().wal().currentSegment());

                    writeTrackedWalIdxsToMetaStore();
                }

                return null;
            });

            CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

            if (grp != null && grp.affinityNode())
                reencryptGroups.put(grpId, pageScanner.pagesCount(grp));

            if (log.isInfoEnabled())
                log.info("New encryption key for group was added [grpId=" + grpId + ", keyId=" + newKeyId + ']');
        }

        startReencryption(encryptionStatus.keySet());
    }

    /**
     * @param grpId Cache group ID.
     * @return Future that will be completed when reencryption of the specified group is finished.
     */
    public IgniteInternalFuture<Void> reencryptionFuture(int grpId) {
        return pageScanner.statusFuture(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @return {@code True} If the specified cache group is currently being re-encrypted.
     */
    public boolean reencryptionInProgress(int grpId) {
        // The method guarantees not only the completion of the re-encryption, but also that the clearing of
        // unused keys is complete.
        return reencryptGroups.containsKey(grpId);
    }

    /**
     * @return Re-encryption rate limit in megabytes per second ({@code 0} - unlimited).
     */
    public double getReencryptionRate() {
        return pageScanner.getRate();
    }

    /**
     * @param rate Re-encryption rate limit in megabytes per second ({@code 0} - unlimited).
     */
    public void setReencryptionRate(double rate) {
        pageScanner.setRate(rate);
    }

    /**
     * Removes encryption key(s).
     *
     * @param grpId Cache group ID.
     */
    private void removeGroupKey(int grpId) {
        synchronized (metaStorageMux) {
            ctx.cache().context().database().checkpointReadLock();

            try {
                if (grpKeys.remove(grpId) == null)
                    return;

                metaStorage.remove(ENCRYPTION_KEYS_PREFIX + grpId);

                if (log.isDebugEnabled())
                    log.debug("Key(s) removed. [grp=" + grpId + "]");
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear meta storage", e);
            }
            finally {
                ctx.cache().context().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Sets new initial group key if key is not null.
     *
     * @param grpId Cache group ID.
     * @param encKey Encryption key
     */
    public void setInitialGroupKey(int grpId, @Nullable byte[] encKey) {
        if (encKey == null || ctx.clientNode())
            return;

        removeGroupKey(grpId);

        withMasterKeyChangeReadLock(() -> {
            addGroupKey(grpId, new GroupKeyEncrypted(INITIAL_KEY_ID, encKey));

            return null;
        });
    }

    /**
     * Callback is called before invalidate page memory.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupStop(int grpId) {
        try {
            reencryptionFuture(grpId).cancel();
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to cancel reencryption [grpId=" + grpId + "]", e);
        }

        reencryptGroups.remove(grpId);
    }

    /**
     * Callback for cache group destroy event.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupDestroyed(int grpId) {
        if (getActiveKey(grpId) == null)
            return;

        removeGroupKey(grpId);
    }

    /**
     * @param grp Cache group.
     * @param partId Partition ID.
     */
    public void onDestroyPartitionStore(CacheGroupContext grp, int partId) {
        if (pageScanner.excludePartition(grp.groupId(), partId))
            setEncryptionState(grp, partId, 0, 0);
    }

    /**
     * @param grp Cache group.
     * @param partId Partition ID.
     */
    public void onCancelDestroyPartitionStore(CacheGroupContext grp, int partId) {
        pageScanner.includePartition(grp.groupId(), partId);
    }

    /**
     * Callback when WAL segment is removed.
     *
     * @param segmentIdx WAL segment index.
     */
    public void onWalSegmentRemoved(long segmentIdx) {
        if (grpKeys.isReleaseWalKeysRequired(segmentIdx))
            ctx.getSystemExecutorService().submit(() -> releaseWalKeys(segmentIdx));
    }

    /**
     * Cleanup keys reserved for WAL reading.
     *
     * @param segmentIdx WAL segment index.
     */
    private void releaseWalKeys(long segmentIdx) {
        withMasterKeyChangeReadLock(() -> {
            synchronized (metaStorageMux) {
                Map<Integer, Set<Integer>> rmvKeys = grpKeys.releaseWalKeys(segmentIdx);

                if (F.isEmpty(rmvKeys))
                    return null;

                try {
                    writeTrackedWalIdxsToMetaStore();

                    for (Map.Entry<Integer, Set<Integer>> entry : rmvKeys.entrySet()) {
                        Integer grpId = entry.getKey();

                        if (reencryptGroups.containsKey(grpId))
                            continue;

                        Set<Integer> keyIds = entry.getValue();

                        if (!grpKeys.removeKeysById(grpId, keyIds))
                            continue;

                        writeGroupKeysToMetaStore(grpId, grpKeys.getAll(grpId));

                        if (log.isInfoEnabled()) {
                            log.info("Previous encryption keys have been removed [grpId=" + grpId +
                                ", keyIds=" + keyIds + ']');
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to remove encryption keys from metastore.", e);
                }
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        try {
            // There is no need to set master key in case of recovery, as it is already relevant.
            if (!restoredFromWAL) {
                String masterKeyName = (String)metastorage.read(MASTER_KEY_NAME_PREFIX);

                if (masterKeyName != null) {
                    if (log.isInfoEnabled())
                        log.info("Master key name loaded from metastrore [masterKeyName=" + masterKeyName + ']');

                    getSpi().setMasterKeyName(masterKeyName);
                }
            }

            metastorage.iterate(ENCRYPTION_KEYS_PREFIX, (key, val) -> {
                int grpId = Integer.parseInt(key.replace(ENCRYPTION_KEYS_PREFIX, ""));

                if (grpKeys.groupIds().contains(grpId))
                    return;

                grpKeys.setGroupKeys(grpId, (List<GroupKeyEncrypted>)val);
            }, true);

            // Try to read keys in previous format.
            if (grpKeys.groupIds().isEmpty()) {
                metastorage.iterate(ENCRYPTION_KEY_PREFIX, (key, val) -> {
                    int grpId = Integer.parseInt(key.replace(ENCRYPTION_KEY_PREFIX, ""));

                    GroupKeyEncrypted grpKey = new GroupKeyEncrypted(INITIAL_KEY_ID, (byte[])val);

                    grpKeys.setGroupKeys(grpId, Collections.singletonList(grpKey));
                }, true);
            }

            Serializable savedSegments = metastorage.read(REENCRYPTED_WAL_SEGMENTS);

            if (savedSegments != null)
                grpKeys.trackedWalSegments((Collection<CacheGroupEncryptionKeys.TrackedWalSegment>)savedSegments);

            if (grpKeys.groupIds().isEmpty()) {
                U.quietAndInfo(log, "Encryption keys loaded from metastore. " +
                    "[grps=" + F.concat(grpKeys.groupIds(), ",") +
                    ", masterKeyName=" + getSpi().getMasterKeyName() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read encryption keys state.", e);
        }

        String newMasterKeyName = IgniteSystemProperties.getString(IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP);

        if (newMasterKeyName != null) {
            if (newMasterKeyName.equals(getSpi().getMasterKeyName())) {
                if (log.isInfoEnabled()) {
                    log.info("Restored master key name equals to name from system property " +
                        IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP + ". This system property will be ignored and " +
                        "recommended to remove [masterKeyName=" + newMasterKeyName + ']');
                }

                return;
            }

            recoveryMasterKeyName = true;

            if (log.isInfoEnabled()) {
                log.info("System property " + IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP + " is set. " +
                    "Master key will be changed locally and group keys will be re-encrypted before join to cluster. " +
                    "Result will be saved to MetaStore on activation process. [masterKeyName=" + newMasterKeyName + ']');
            }

            getSpi().setMasterKeyName(newMasterKeyName);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metaStorage) throws IgniteCheckedException {
        withMasterKeyChangeReadLock(() -> {
            synchronized (metaStorageMux) {
                this.metaStorage = metaStorage;

                writeToMetaStoreEnabled = true;

                if (recoveryMasterKeyName)
                    writeKeysToWal();

                writeKeysToMetaStore(restoredFromWAL || recoveryMasterKeyName);

                restoredFromWAL = false;

                recoveryMasterKeyName = false;
            }

            return null;
        });

        for (Map.Entry<Integer, Integer> entry : reencryptGroupsForced.entrySet()) {
            int grpId = entry.getKey();

            if (reencryptGroups.containsKey(grpId))
                continue;

            if (entry.getValue() != getActiveKey(grpId).unsignedId())
                continue;

            CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

            if (grp == null || !grp.affinityNode())
                continue;

            long[] offsets = pageScanner.pagesCount(grp);

            reencryptGroups.put(grpId, offsets);
        }

        reencryptGroupsForced.clear();
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        withMasterKeyChangeReadLock(() -> {
            synchronized (metaStorageMux) {
                writeToMetaStoreEnabled = metaStorage != null;

                if (writeToMetaStoreEnabled)
                    writeKeysToMetaStore(false);
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        synchronized (metaStorageMux) {
            writeToMetaStoreEnabled = false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.activateCluster() || fut.localJoinExchange()) {
            try {
                startReencryption(reencryptGroups.keySet());
            }
            catch (IgniteCheckedException e) {
                log.error("Unable to start reencryption", e);
            }
        }
    }

    /**
     * Set reencryption status for partition.
     *
     * @param grp Cache group.
     * @param partId Partition ID.
     * @param idx Index of the last reencrypted page.
     * @param total Total pages to be reencrypted.
     */
    public void setEncryptionState(CacheGroupContext grp, int partId, int idx, int total) {
        // The last element of the array is used to store the status of the index partition.
        long[] states = reencryptGroups.computeIfAbsent(grp.groupId(), v -> new long[grp.affinity().partitions() + 1]);

        states[Math.min(partId, states.length - 1)] = ReencryptStateUtils.state(idx, total);
    }

    /**
     * Get reencryption status for partition.
     *
     * @param grpId Cache group ID.
     * @param partId Parttiion ID.
     * @return Index and count of pages to be reencrypted.
     */
    public long getEncryptionState(int grpId, int partId) {
        long[] states = reencryptGroups.get(grpId);

        if (states == null)
            return 0;

        return states[Math.min(partId, states.length - 1)];
    }

    /**
     * @param grpId Cache group ID.
     * @return The number of bytes left for re-ecryption.
     */
    public long getBytesLeftForReencryption(int grpId) {
        return pageScanner.remainingPagesCount(grpId) * ctx.config().getDataStorageConfiguration().getPageSize();
    }

    /**
     * @param keyCnt Count of keys to generate.
     * @return Future that will contain results of generation.
     */
    public IgniteInternalFuture<T2<Collection<byte[]>, byte[]>> generateKeys(int keyCnt) {
        if (keyCnt == 0 || !ctx.clientNode())
            return new GridFinishedFuture<>(createKeys(keyCnt));

        synchronized (opsMux) {
            if (disconnected || stopped) {
                return new GridFinishedFuture<>(
                    new IgniteFutureCancelledException("Node " + (stopped ? "stopped" : "disconnected")));
            }

            try {
                GenerateEncryptionKeyFuture genEncKeyFut = new GenerateEncryptionKeyFuture(keyCnt);

                sendGenerateEncryptionKeyRequest(genEncKeyFut);

                genEncKeyFuts.put(genEncKeyFut.id(), genEncKeyFut);

                return genEncKeyFut;
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }
    }

    /** */
    private void sendGenerateEncryptionKeyRequest(GenerateEncryptionKeyFuture fut) throws IgniteCheckedException {
        ClusterNode rndNode = U.randomServerNode(ctx);

        if (rndNode == null)
            throw new IgniteCheckedException("There is no node to send GenerateEncryptionKeyRequest to");

        GenerateEncryptionKeyRequest req = new GenerateEncryptionKeyRequest(fut.keyCount());

        fut.id(req.id());
        fut.nodeId(rndNode.id());

        ctx.io().sendToGridTopic(rndNode.id(), TOPIC_GEN_ENC_KEY, req, SYSTEM_POOL);
    }

    /**
     * Suspend re-encryption of the cache group.
     *
     * @param grpId Cache group ID.
     */
    public boolean suspendReencryption(int grpId) throws IgniteCheckedException {
        return reencryptionFuture(grpId).cancel();
    }

    /**
     * Forces re-encryption of the cache group.
     *
     * @param grpId Cache group ID.
     */
    public boolean resumeReencryption(int grpId) throws IgniteCheckedException {
        if (!reencryptionFuture(grpId).isDone())
            return false;

        if (!reencryptionInProgress(grpId))
            throw new IgniteCheckedException("Re-encryption completed or not required [grpId=" + grpId + "]");

        startReencryption(Collections.singleton(grpId));

        return true;
    }

    /**
     * @param grpIds Cache group IDs.
     * @throws IgniteCheckedException If failed.
     */
    private void startReencryption(Collection<Integer> grpIds) throws IgniteCheckedException {
        for (int grpId : grpIds) {
            IgniteInternalFuture<?> fut = pageScanner.schedule(grpId);

            fut.listen(f -> {
                if (f.isCancelled() || f.error() != null) {
                    log.warning("Reencryption " +
                        (f.isCancelled() ? "cancelled" : "failed") + " [grp=" + grpId + "]", f.error());

                    return;
                }

                withMasterKeyChangeReadLock(() -> {
                    synchronized (metaStorageMux) {
                        cleanupKeys(grpId);

                        reencryptGroups.remove(grpId);
                    }

                    return null;
                });
            });
        }
    }

    /**
     * @param grpId Cache group ID.
     * @throws IgniteCheckedException If failed.
     */
    private void cleanupKeys(int grpId) throws IgniteCheckedException {
        Set<Integer> rmvKeyIds = grpKeys.removeUnusedKeys(grpId);

        if (rmvKeyIds.isEmpty())
            return;

        writeGroupKeysToMetaStore(grpId, grpKeys.getAll(grpId));

        if (log.isInfoEnabled())
            log.info("Previous encryption keys were removed [grpId=" + grpId + ", keyIds=" + rmvKeyIds + ']');
    }

    /**
     * Writes all unsaved grpEncKeys to metaStorage.
     *
     * @param writeAll {@code True} if force rewrite all keys.
     * @throws IgniteCheckedException If failed.
     */
    private void writeKeysToMetaStore(boolean writeAll) throws IgniteCheckedException {
        if (writeAll)
            metaStorage.write(MASTER_KEY_NAME_PREFIX, getSpi().getMasterKeyName());

        if (!reencryptGroupsForced.isEmpty())
            writeTrackedWalIdxsToMetaStore();

        for (Integer grpId : grpKeys.groupIds()) {
            if (!writeAll && !reencryptGroupsForced.containsKey(grpId) &&
                metaStorage.read(ENCRYPTION_KEYS_PREFIX + grpId) != null)
                continue;

            writeGroupKeysToMetaStore(grpId, grpKeys.getAll(grpId));
        }
    }

    /**
     * Writes cache group encryption keys to metastore.
     *
     * @param grpId Cache group ID.
     */
    private void writeGroupKeysToMetaStore(int grpId, List<GroupKeyEncrypted> keys) throws IgniteCheckedException {
        assert Thread.holdsLock(metaStorageMux);

        if (metaStorage == null || !writeToMetaStoreEnabled || stopped)
            return;

        ctx.cache().context().database().checkpointReadLock();

        try {
            metaStorage.write(ENCRYPTION_KEYS_PREFIX + grpId, (Serializable)keys);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Writes tracked (encrypted with previous encryption keys) WAL segments to metastore.
     */
    private void writeTrackedWalIdxsToMetaStore() throws IgniteCheckedException {
        assert Thread.holdsLock(metaStorageMux);

        if (metaStorage == null || !writeToMetaStoreEnabled || stopped)
            return;

        ctx.cache().context().database().checkpointReadLock();

        try {
            metaStorage.write(REENCRYPTED_WAL_SEGMENTS, (Serializable)grpKeys.trackedWalSegments());
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Checks cache encryption supported by all nodes in cluster.
     *
     * @throws IgniteCheckedException If check fails.
     */
    public void checkEncryptedCacheSupported() throws IgniteCheckedException {
        Collection<ClusterNode> nodes = ctx.grid().cluster().nodes();

        for (ClusterNode node : nodes) {
            if (CACHE_ENCRYPTION_SINCE.compareTo(node.version()) > 0) {
                throw new IgniteCheckedException("All nodes in cluster should be 2.7.0 or greater " +
                    "to create encrypted cache! [nodeId=" + node.id() + "]");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataExchangeType discoveryDataType() {
        return ENCRYPTION_MGR;
    }

    /**
     * @param knownKeys Saved keys set.
     * @return New keys for local cache groups.
     */
    @Nullable private HashMap<Integer, byte[]> newEncryptionKeys(Set<Integer> knownKeys) {
        assert !isMasterKeyChangeInProgress();

        Map<Integer, CacheGroupDescriptor> grpDescs = ctx.cache().cacheGroupDescriptors();

        HashMap<Integer, byte[]> newKeys = null;

        for (CacheGroupDescriptor grpDesc : grpDescs.values()) {
            if (knownKeys.contains(grpDesc.groupId()) || !grpDesc.config().isEncryptionEnabled())
                continue;

            if (newKeys == null)
                newKeys = new HashMap<>();

            newKeys.put(grpDesc.groupId(), getSpi().encryptKey(getSpi().create()));
        }

        return newKeys;
    }

    /**
     * Generates required count of encryption keys.
     *
     * @param keyCnt Keys count.
     * @return Tuple of collection with newly generated encryption keys and master key digest.
     */
    T2<Collection<byte[]>, byte[]> createKeys(int keyCnt) {
        return withMasterKeyChangeReadLock(() -> {
            if (keyCnt == 0)
                return new T2<>(Collections.emptyList(), getSpi().masterKeyDigest());

            List<byte[]> encKeys = new ArrayList<>(keyCnt);

            for (int i = 0; i < keyCnt; i++)
                encKeys.add(getSpi().encryptKey(getSpi().create()));

            return new T2<>(encKeys, getSpi().masterKeyDigest());
        });
    }

    /**
     * Sets up master key and re-encrypt group keys. Writes changes to WAL and MetaStorage.
     *
     * @param name New master key name.
     */
    private void doChangeMasterKey(String name) {
        if (log.isInfoEnabled())
            log.info("Start master key change [masterKeyName=" + name + ']');

        masterKeyChangeLock.writeLock().lock();

        try {
            getSpi().setMasterKeyName(name);

            synchronized (metaStorageMux) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    writeKeysToWal();

                    assert writeToMetaStoreEnabled;

                    writeKeysToMetaStore(true);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }

            if (log.isInfoEnabled())
                log.info("Master key successfully changed [masterKeyName=" + name + ']');
        }
        catch (Exception e) {
            U.error(log, "Unable to change master key locally.", e);

            ctx.failure().process(new FailureContext(CRITICAL_ERROR,
                new IgniteException("Unable to change master key locally.", e)));
        }
        finally {
            masterKeyChangeLock.writeLock().unlock();
        }
    }

    /** Writes the record with the master key name and all keys to WAL. */
    private void writeKeysToWal() throws IgniteCheckedException {
        List<T2<Integer, GroupKeyEncrypted>> reencryptedKeys = new ArrayList<>();

        for (int grpId : grpKeys.groupIds()) {
            for (GroupKeyEncrypted grpKey : grpKeys.getAll(grpId))
                reencryptedKeys.add(new T2<>(grpId, grpKey));
        }

        MasterKeyChangeRecordV2 rec = new MasterKeyChangeRecordV2(getSpi().getMasterKeyName(), reencryptedKeys);

        WALPointer ptr = ctx.cache().context().wal().log(rec);

        assert ptr != null;
    }

    /**
     * Apply keys from WAL record during the recovery phase.
     *
     * @param rec Record.
     */
    public void applyKeys(MasterKeyChangeRecordV2 rec) {
        assert !writeToMetaStoreEnabled && !ctx.state().clusterState().active();

        if (log.isInfoEnabled())
            log.info("Master key name loaded from WAL [masterKeyName=" + rec.getMasterKeyName() + ']');

        try {
            getSpi().setMasterKeyName(rec.getMasterKeyName());

            Map<Integer, List<GroupKeyEncrypted>> keysMap = new HashMap<>();

            for (T2<Integer, GroupKeyEncrypted> entry : rec.getGrpKeys())
                keysMap.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());

            for (Map.Entry<Integer, List<GroupKeyEncrypted>> entry : keysMap.entrySet())
                grpKeys.setGroupKeys(entry.getKey(), entry.getValue());

            restoredFromWAL = true;
        } catch (IgniteSpiException e) {
            log.warning("Unable to apply group keys from WAL record [masterKeyName=" + rec.getMasterKeyName() + ']', e);
        }
    }

    /**
     * Start reencryption using logical WAL record.
     *
     * @param rec Reencryption start logical record.
     */
    public void applyReencryptionStartRecord(ReencryptionStartRecord rec) {
        assert !writeToMetaStoreEnabled;

        for (Map.Entry<Integer, Byte> e : rec.groups().entrySet())
            reencryptGroupsForced.put(e.getKey(), e.getValue() & 0xff);
    }

    /**
     * Prepares master key change. Checks master key consistency.
     *
     * @param req Request.
     * @return Result future.
     */
    private IgniteInternalFuture<EmptyResult> prepareMasterKeyChange(MasterKeyChangeRequest req) {
        if (masterKeyChangeRequest != null) {
            return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. " +
                "The previous change was not completed."));
        }

        masterKeyChangeRequest = req;

        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        try {
            String masterKeyName = decryptKeyName(req.encKeyName());

            if (masterKeyName.equals(getMasterKeyName()))
                throw new IgniteException("Master key change was rejected. New name equal to the current.");

            byte[] digest = tryChangeMasterKey(masterKeyName);

            if (!Arrays.equals(req.digest, digest)) {
                return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. Master " +
                    "key digest consistency check failed. Make sure that the new master key is the same at " +
                    "all server nodes [nodeId=" + ctx.localNodeId() + ']'));
            }
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(new IgniteException("Master key change was rejected [nodeId=" +
                ctx.localNodeId() + ']', e));
        }

        return new GridFinishedFuture<>(new EmptyResult());
    }

    /**
     * Starts master key change process if there are no errors.
     *
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void finishPrepareMasterKeyChange(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
        if (!err.isEmpty()) {
            if (masterKeyChangeRequest != null && masterKeyChangeRequest.requestId().equals(id))
                masterKeyChangeRequest = null;

            completeMasterKeyChangeFuture(id, err);
        }
        else if (U.isLocalNodeCoordinator(ctx.discovery()))
            performMKChangeProc.start(id, masterKeyChangeRequest);
    }

    /**
     * Changes master key.
     *
     * @param req Request.
     * @return Result future.
     */
    private IgniteInternalFuture<EmptyResult> performMasterKeyChange(MasterKeyChangeRequest req) {
        if (masterKeyChangeRequest == null || !masterKeyChangeRequest.equals(req))
            return new GridFinishedFuture<>(new IgniteException("Unknown master key change was rejected."));

        if (!ctx.state().clusterState().active()) {
            masterKeyChangeRequest = null;

            return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. " +
                "The cluster is inactive."));
        }

        if (!ctx.clientNode())
            doChangeMasterKey(decryptKeyName(req.encKeyName()));

        masterKeyChangeRequest = null;

        masterKeyDigest = req.digest();

        return new GridFinishedFuture<>(new EmptyResult());
    }

    /**
     * Finishes master key change.
     *
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void finishPerformMasterKeyChange(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
        completeMasterKeyChangeFuture(id, err);
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     */
    private void completeMasterKeyChangeFuture(UUID reqId, Map<UUID, Exception> err) {
        synchronized (opsMux) {
            boolean isInitiator = masterKeyChangeFut != null && masterKeyChangeFut.id().equals(reqId);

            if (!isInitiator || masterKeyChangeFut.isDone())
                return;

            if (!F.isEmpty(err)) {
                Exception e = err.values().stream().findFirst().get();

                masterKeyChangeFut.onDone(e);
            }
            else
                masterKeyChangeFut.onDone();

            masterKeyChangeFut = null;
        }
    }

    /**
     * @param msg Error message.
     */
    private void cancelFutures(String msg) {
        assert Thread.holdsLock(opsMux);

        for (GenerateEncryptionKeyFuture fut : genEncKeyFuts.values())
            fut.onDone(new IgniteFutureCancelledException(msg));

        if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone())
            masterKeyChangeFut.onDone(new IgniteFutureCancelledException(msg));

        if (grpKeyChangeProc != null)
            grpKeyChangeProc.cancel(msg);
    }

    /** @return {@code True} if the master key change process in progress. */
    public boolean isMasterKeyChangeInProgress() {
        return masterKeyChangeRequest != null;
    }

    /**
     * Digest of last changed master key or {@code null} if master key was not changed.
     * <p>
     * Used to verify the digest on a client node in case of cache start after master key change.
     *
     * @return Digest of last changed master key or {@code null} if master key was not changed.
     */
    @Nullable public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /**
     * @param c Callable to run with master key change read lock.
     * @return Computed result.
     */
    <T> T withMasterKeyChangeReadLock(Callable<T> c) {
        masterKeyChangeLock.readLock().lock();

        try {
            return c.call();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
    }

    /**
     * @param masterKeyName Master key name.
     * @return Master key digest.
     * @throws IgniteException if unable to get master key digest.
     */
    private byte[] masterKeyDigest(String masterKeyName) {
        return getSpi().masterKeyDigest(masterKeyName);
    }

    /**
     * Tries to change the active master key with the given one and returns its digest.
     *
     * @param masterKeyName Master key name.
     * @return Master key digest.
     * @throws IgniteCheckedException If the master key cannot be changed for some reason.
     */
    private byte[] tryChangeMasterKey(String masterKeyName) throws IgniteCheckedException {
        byte[] digest;

        masterKeyChangeLock.writeLock().lock();

        try {
            String curName = getSpi().getMasterKeyName();

            try {
                getSpi().setMasterKeyName(masterKeyName);

                digest = getSpi().masterKeyDigest();
            } catch (Exception e) {
                throw new IgniteException("Unable to set master key locally [masterKeyName=" + masterKeyName + ']', e);
            } finally {
                getSpi().setMasterKeyName(curName);
            }
        }
        finally {
            masterKeyChangeLock.writeLock().unlock();
        }

        return digest;
    }

    /**
     * @param keyName Master key name to encrypt.
     * @return Encrypted master key name.
     */
    private byte[] encryptKeyName(String keyName) {
        return withMasterKeyChangeReadLock(() -> {
            Serializable key = getSpi().create();

            byte[] encKey = getSpi().encryptKey(key);

            byte[] serKeyName = U.toBytes(keyName);

            ByteBuffer res = ByteBuffer.allocate(/*Encrypted key length*/4 + encKey.length +
                getSpi().encryptedSize(serKeyName.length));

            res.putInt(encKey.length);
            res.put(encKey);

            getSpi().encrypt(ByteBuffer.wrap(serKeyName), key, res);

            return res.array();
        });
    }

    /**
     * @param data Byte array with encrypted a master key name.
     * @return Decrypted master key name.
     */
    private String decryptKeyName(byte[] data) {
        return withMasterKeyChangeReadLock(() -> {
            ByteBuffer buf = ByteBuffer.wrap(data);

            int keyLen = buf.getInt();

            byte[] encKey = new byte[keyLen];

            buf.get(encKey);

            byte[] encKeyName = new byte[buf.remaining()];

            buf.get(encKeyName);

            byte[] serKeyName = getSpi().decrypt(encKeyName, getSpi().decryptKey(encKey));

            return U.fromBytes(serKeyName);
        });
    }

    /** Master key change request. */
    private static class MasterKeyChangeRequest implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Request id. */
        private final UUID reqId;

        /** Encrypted master key name. */
        private final byte[] encKeyName;

        /** Master key digest. */
        private final byte[] digest;

        /**
         * @param reqId Request id.
         * @param encKeyName Encrypted master key name.
         * @param digest Master key digest.
         */
        private MasterKeyChangeRequest(UUID reqId, byte[] encKeyName, byte[] digest) {
            this.reqId = reqId;
            this.encKeyName = encKeyName;
            this.digest = digest;
        }

        /** @return Request id. */
        UUID requestId() {
            return reqId;
        }

        /** @return Encrypted master key name. */
        byte[] encKeyName() {
            return encKeyName;
        }

        /** @return Master key digest. */
        byte[] digest() {
            return digest;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof MasterKeyChangeRequest))
                return false;

            MasterKeyChangeRequest key = (MasterKeyChangeRequest)o;

            return Arrays.equals(encKeyName, key.encKeyName) &&
                Arrays.equals(digest, key.digest) &&
                Objects.equals(reqId, key.reqId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = Objects.hash(reqId);

            res = 31 * res + Arrays.hashCode(encKeyName);
            res = 31 * res + Arrays.hashCode(digest);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MasterKeyChangeRequest.class, this);
        }
    }

    /** */
    protected static class EmptyResult implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /** */
    protected static class NodeEncryptionKeys implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        NodeEncryptionKeys(
            HashMap<Integer, List<GroupKeyEncrypted>> knownKeysWithIds,
            Map<Integer, byte[]> newKeys,
            byte[] masterKeyDigest
        ) {
            this.newKeys = newKeys;
            this.masterKeyDigest = masterKeyDigest;

            if (F.isEmpty(knownKeysWithIds))
                return;

            // To be able to join the old cluster.
            knownKeys = U.newHashMap(knownKeysWithIds.size());

            for (Map.Entry<Integer, List<GroupKeyEncrypted>> entry : knownKeysWithIds.entrySet())
                knownKeys.put(entry.getKey(), entry.getValue().get(0).key());

            this.knownKeysWithIds = knownKeysWithIds;
        }

        /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node (in compatible format). */
        Map<Integer, byte[]> knownKeys;

        /**  New keys i.e. keys for a local statically configured caches. */
        Map<Integer, byte[]> newKeys;

        /** Master key digest. */
        byte[] masterKeyDigest;

        /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node. */
        Map<Integer, List<GroupKeyEncrypted>> knownKeysWithIds;
    }

    /** */
    private class GenerateEncryptionKeyFuture extends GridFutureAdapter<T2<Collection<byte[]>, byte[]>> {
        /** */
        private IgniteUuid id;

        /** */
        private int keyCnt;

        /** */
        private UUID nodeId;

        /**
         * @param keyCnt Count of keys to generate.
         */
        private GenerateEncryptionKeyFuture(int keyCnt) {
            this.keyCnt = keyCnt;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable T2<Collection<byte[]>, byte[]> res, @Nullable Throwable err) {
            // Make sure to remove future before completion.
            genEncKeyFuts.remove(id, this);

            return super.onDone(res, err);
        }

        /** */
        public IgniteUuid id() {
            return id;
        }

        /** */
        public void id(IgniteUuid id) {
            this.id = id;
        }

        /** */
        public UUID nodeId() {
            return nodeId;
        }

        /** */
        public void nodeId(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** */
        public int keyCount() {
            return keyCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GenerateEncryptionKeyFuture.class, this);
        }
    }

    /** Key change future. */
    protected static class KeyChangeFuture extends GridFutureAdapter<Void> {
        /** Request ID. */
        private final UUID id;

        /** @param id Request ID. */
        KeyChangeFuture(UUID id) {
            this.id = id;
        }

        /** @return Request ID. */
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeyChangeFuture.class, this);
        }
    }
}
