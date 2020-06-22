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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.EncryptionStatusRecord;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
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
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ACTIVE_KEY_ID_FOR_GROUP;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ENCRYPTION_MGR;
import static org.apache.ignite.internal.GridTopic.TOPIC_GEN_ENC_KEY;
import static org.apache.ignite.internal.IgniteFeatures.GROUP_KEY_CHANGE;
import static org.apache.ignite.internal.IgniteFeatures.MASTER_KEY_CHANGE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.GROUP_KEY_CHANGE_FINISH;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.GROUP_KEY_CHANGE_PREPARE;
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
 *         <li>2. Generate(but doesn't store locally!) and send keys for all statically configured groups in case the not presented in metastore.</li>
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
public class GridEncryptionManager extends GridManagerAdapter<EncryptionSpi> implements MetastorageLifecycleListener,
    IgniteChangeGlobalStateSupport, IgniteEncryption {
    /**
     * Cache encryption introduced in this Ignite version.
     */
    private static final IgniteProductVersion CACHE_ENCRYPTION_SINCE = IgniteProductVersion.fromString("2.7.0");

    /** Prefix for a master key name. */
    public static final String MASTER_KEY_NAME_PREFIX = "encryption-master-key-name";

    /** Prefix for a encryption group key in meta store, which contains encryption keys with identifiers. */
    public static final String ENCRYPTION_KEYS_PREFIX = "grp-encryption-keys-";

    /** Prefix for a encryption group key in meta store, which contains encryption keys identifiers. */
    private static final String ENCRYPTION_ACTIVE_KEY_PREFIX = "grp-encryption-active-key-";

    /** The name on the meta store key, that contains wal segments encrypted using previous encryption keys. */
    private static final String REENCRYPTED_WAL_SEGMENTS = "reencrypted-wal-segments";

    /** Prefix for a encryption group key in meta store. */
    @Deprecated
    private static final String ENCRYPTION_KEY_PREFIX = "grp-encryption-key-";

    /** Initial identifier for cache group encryption key. */
    private static final int INITIAL_KEY_IDENTIFIER = 0;

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

    /** Group encryption keys. */
    private final Map<Integer, Map<Integer, Serializable>> grpEncKeys = new ConcurrentHashMap<>();

    /** Key IDs that are used for writing. */
    private final ConcurrentHashMap<Integer, Integer> grpEncActiveIds = new ConcurrentHashMap<>();

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
    private volatile KeyChangeFuture masterKeyChangeFut;

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

    /** Two phase distributed process, that performs encryption group key rotation. */
    private final GroupKeyChangeProcess grpKeyChangeProc = new GroupKeyChangeProcess();

    /** Cache groups for which encryption key was changed. */
    private final Set<Integer> reencryptGroups = new GridConcurrentHashSet<>();

    /** Cache groups for which encryption key was changed manually. */
    private final Set<Integer> reencryptGroupsForced = new GridConcurrentHashSet<>();

    // todo walidx -> <grp -> keyIds>
    private final Map<Long, Map<Integer, Set<Integer>>> walSegments = new ConcurrentHashMap<>();

    /** Cache group reencryption manager. */
    private CacheGroupReencryption reencryption;

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

        reencryption = new CacheGroupReencryption(ctx);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        // Stop re-encryption.
        reencryption.stop();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        // No-op.
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

            // todo
            grpKeyChangeProc.grpKeyChangeReq = null;

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
        if (!isCoordinator())
            return;

        //We can't store keys before node join to cluster(on statically configured cache registration).
        //Because, keys should be received from cluster.
        //Otherwise, we would generate different keys on each started node.
        //So, after starting, coordinator saves locally newly generated encryption keys.
        //And sends that keys to every joining node.
        synchronized (metaStorageMux) {
            //Keys read from meta storage.
            HashMap<Integer, T2<Integer, byte[]>> knownEncKeys = knownEncryptionKeys();

            //Generated(not saved!) keys for a new caches.
            //Configured statically in config, but doesn't stored on the disk.
            HashMap<Integer, byte[]> newEncKeys =
                newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

            if (newEncKeys == null)
                return;

            //We can store keys to the disk, because we are on a coordinator.
            for (Map.Entry<Integer, byte[]> entry : newEncKeys.entrySet()) {
                groupKey(entry.getKey(), entry.getValue(), INITIAL_KEY_IDENTIFIER);

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

        // todo verify topology when performing?
        if (grpKeyChangeProc.inProgress()) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Group key change is in progress! Node join is rejected. [node=" + node.id() + "]",
                "Group key change is in progress! Node join is rejected.");
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

        if (!IgniteFeatures.nodeSupports(node, GROUP_KEY_CHANGE)) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Joining node doesn't support multiple encryption keys for single group [node=" + node.id() + "]",
                "Joining node doesn't support multiple encryption keys for single group.");
        }

        if (F.isEmpty(nodeEncKeys.knownKeys)) {
            U.quietAndInfo(log, "Joining node doesn't have stored group keys [node=" + node.id() + "]");

            return null;
        }

        assert !F.isEmpty(nodeEncKeys.knownKeyIds);

        for (Map.Entry<Integer, byte[]> entry : nodeEncKeys.knownKeys.entrySet()) {
            int grpId = entry.getKey();

            GroupKey locEncKey = groupKey(grpId);

            if (locEncKey == null)
                continue;

            Serializable rmtKey = getSpi().decryptKey(entry.getValue());

            if (F.eq(locEncKey.key(), rmtKey) && F.eq(locEncKey.id(), nodeEncKeys.knownKeyIds.get(grpId)))
                continue;

            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Cache key differs! Node join is rejected. [node=" + node.id() + ", grp=" + entry.getKey() + "]",
                "Cache key differs! Node join is rejected.");
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        if (dataBag.isJoiningNodeClient())
            return;

        HashMap<Integer, T2<Integer, byte[]>> knownEncKeys = knownEncryptionKeys();

        HashMap<Integer, byte[]> newKeys =
            newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

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
            if (groupKey(entry.getKey()) == null) {
                U.quietAndInfo(log, "Store group key received from joining node [node=" +
                        data.joiningNodeId() + ", grp=" + entry.getKey() + "]");

                groupKey(entry.getKey(), entry.getValue(), INITIAL_KEY_IDENTIFIER);
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

        HashMap<Integer, T2<Integer, byte[]>> knownEncKeys = knownEncryptionKeys();

        HashMap<Integer, byte[]> newKeys =
            newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

        if (newKeys != null) {
            for (Map.Entry<Integer, byte[]> entry : newKeys.entrySet()) {
                T2 old = knownEncKeys.putIfAbsent(entry.getKey(), new T2<>(0, entry.getValue()));

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
            if (groupKey(entry.getKey()) == null) {
                U.quietAndInfo(log, "Store group key received from coordinator [grp=" + entry.getKey() + "]");

                if (entry.getValue() instanceof T2) {
                    T2<Integer, byte[]> pair = (T2<Integer, byte[]>)entry.getValue();

                    groupKey(entry.getKey(), pair.get2(), pair.get1());

                    continue;
                }

                // compatibility
                groupKey(entry.getKey(), (byte[])entry.getValue(), INITIAL_KEY_IDENTIFIER);
            }
            else {
                U.quietAndInfo(log, "Skip group key received from coordinator. Already exists. [grp=" +
                    entry.getKey() + "]");
            }
        }
    }

    /**
     * Returns group encryption key.
     *
     * @param grpId Cache group ID.
     * @return Group encryption key with identifier, that was set for writing.
     */
    @Nullable public GroupKey groupKey(int grpId) {
        Map<Integer, Serializable> keyMap = grpEncKeys.get(grpId);

        if (F.isEmpty(keyMap))
            return null;

        int keyId = grpEncActiveIds.get(grpId);

        return new GroupKey(keyMap.get(keyId), keyId);
    }

    /**
     * Returns group encryption key with specified identifier.
     *
     * @param grpId Cache group ID.
     * @param keyId Key ID.
     * @return Group encryption key.
     */
    @Nullable public Serializable groupKey(int grpId, int keyId) {
        Map<Integer, Serializable> keys = grpEncKeys.get(grpId);

        if (keys == null)
            return null;

        return keys.get(keyId);
    }

    @Nullable public Map<Integer, Integer> groupKeysInfo(int grpId) {
        Map<Integer, Serializable> map = grpEncKeys.get(grpId);

        if (map == null)
            return null;

        Map<Integer, Integer> keysInfo = new TreeMap<>();

        for (Map.Entry<Integer, Serializable> entry : map.entrySet()) {
            byte[] bytes = U.toBytes(entry.getValue());

            keysInfo.put(entry.getKey(), Arrays.hashCode(bytes));
        }

        return keysInfo;
    }

    /**
     * Store group encryption key.
     *
     * @param grpId Cache group ID.
     * @param encGrpKey Encrypted group key.
     * @param keyId Key ID.
     */
    private void groupKey(int grpId, byte[] encGrpKey, int keyId) {
        try {
            replaceKey(grpId, encGrpKey, keyId, true, true);
        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to write cache group encryption key [grpId=" + grpId + ']', e);
        }
    }

    // todo sync properly
    public void replaceKey(
        int grpId,
        byte[] encGrpKey,
        int keyId,
        boolean newKey,
        boolean activeKey
    ) throws IgniteCheckedException {
        Serializable encKey = withMasterKeyChangeReadLock(() -> getSpi().decryptKey(encGrpKey));

        boolean walSegmentsUpdated = false;

        synchronized (metaStorageMux) {
            if (newKey)
                grpEncKeys.computeIfAbsent(grpId, m -> new ConcurrentHashMap<>()).put(keyId, encKey);

            if (activeKey) {
                Integer prevKeyId = grpEncActiveIds.put(grpId, keyId);

                if (prevKeyId != null) {
                    assert prevKeyId != keyId : "keyId=" + keyId;

                    long walIdx = ctx.cache().context().wal().currentSegment();

                    walSegmentsUpdated = walSegments.computeIfAbsent(walIdx, v -> new HashMap<>())
                        .computeIfAbsent(grpId, v -> new HashSet<>()).add(prevKeyId);
                }
            }

            writeToMetaStore(grpId, newKey, activeKey, walSegmentsUpdated);
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
                "Unable to get the master key digest."));
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

            if (!grpKeyChangeProc.finished()) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Master key change was rejected. " +
                    "Cache encryption key change is in progress."));
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
    @Override public IgniteFuture<Void> changeGroupKey(Collection<String> cacheOrGrpNames) {
        if (ctx.clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), GROUP_KEY_CHANGE))
            throw new IllegalStateException("Not all nodes in the cluster support this operation.");

        if (!ctx.state().clusterState().active())
            throw new IgniteException("Operation was rejected. The cluster is inactive.");

        DiscoCache discoCache = ctx.discovery().discoCache();

        int bltSize = discoCache.baselineNodes().size();
        int bltOnline = discoCache.aliveBaselineNodes().size();

        if (bltSize != bltOnline)
            throw new IgniteException("Not all baseline nodes online [total=" + bltSize + ", online=" + bltOnline + "]");

        int[] groups = new int[cacheOrGrpNames.size()];
        byte[][] keys = new byte[groups.length][];
        byte[] keyIds = new byte[groups.length];

        int n = 0;

        for (String cacheOrGroupName : cacheOrGrpNames) {
            CacheGroupContext grp = ctx.cache().cacheGroup(CU.cacheId(cacheOrGroupName));

            if (grp == null) {
                IgniteInternalCache cache = ctx.cache().cache(cacheOrGroupName);

                if (cache == null)
                    throw new IgniteException("Cache or group " + cacheOrGroupName + " doesn't exists");

                grp = cache.context().group();

                if (grp.sharedGroup()) {
                    throw new IgniteException("Cache " + cacheOrGroupName + " is a part of group " + grp.name() +
                        ". Provide group name instead of cache name for shared groups.");
                }
            }

            if (!encryptionTask(grp.groupId()).isDone())
                throw new IgniteException("Reencryption is in progress [grp=" + cacheOrGroupName + "]");

            groups[n] = grp.groupId();
            keys[n] = getSpi().encryptKey(getSpi().create());
            keyIds[n] = (byte)(grpEncActiveIds.get(grp.groupId()) + 1);

            n += 1;
        }

        AffinityTopologyVersion curVer = ctx.cache().context().exchange().readyAffinityVersion();
        ChangeCacheEncryptionRequest req = new ChangeCacheEncryptionRequest(groups, keys, keyIds, curVer);

        IgniteFuture<Void> fut;

        synchronized (opsMux) {
            fut = grpKeyChangeProc.start(req);
        }

        return fut;
    }

    /**
     * @param grpId Cache group ID.
     * @return Future that will be completed when re-encryption of specified group is finished.
     */
    public IgniteInternalFuture<Void> encryptionTask(int grpId) {
        return reencryption.encryptionFuture(grpId);
    }

    /**
     * Removes encryption key.
     *
     * @param grpId Cache group ID.
     */
    private void removeGroupKey(int grpId) {
        synchronized (metaStorageMux) {
            ctx.cache().context().database().checkpointReadLock();

            try {
                grpEncKeys.remove(grpId);

                metaStorage.remove(ENCRYPTION_KEYS_PREFIX + grpId);
                metaStorage.remove(ENCRYPTION_ACTIVE_KEY_PREFIX + grpId);

                if (log.isDebugEnabled())
                    log.debug("Key removed. [grp=" + grpId + "]");
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
     * Callback for cache group start event.
     *
     * @param grpId  Cache group ID.
     * @param encKey Encryption key
     */
    public void beforeCacheGroupStart(int grpId, @Nullable byte[] encKey) {
        if (encKey == null || ctx.clientNode())
            return;

        groupKey(grpId, encKey, INITIAL_KEY_IDENTIFIER);
    }

    /**
     * Callback is called before invalidate page memory.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupStop(int grpId) {
        IgniteInternalFuture fut = encryptionTask(grpId);

        if (!fut.isDone()) {
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.warning("Unable to cancel re-encryption [grpId=" + grpId + "]", e);
            }
        }
    }

    /**
     * Callback for cache group destroy event.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupDestroyed(int grpId) {
        if (groupKey(grpId) == null)
            return;

        removeGroupKey(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public void onDestroyPartitionStore(int grpId, int partId) {
        try {
            if (!reencryption.cancel(grpId, partId))
                return;

            FilePageStoreManager mgr = (FilePageStoreManager)ctx.cache().context().pageStore();

            PageStore pageStore = mgr.getStore(grpId, partId);

            pageStore.encryptPageIndex(0);
            pageStore.encryptPageCount(0);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to cancel re-encryption [grpId=" + grpId + ", partId=" + partId + "]", e);
        }
    }

    /**
     * Callabck when WAL segment is removed.
     *
     * @param segmentIdx WAL segment index.
     */
    public void onWalSegmentRemoved(long segmentIdx) {
        // todo remove
        if (log.isDebugEnabled())
            log.debug(">>> removed segment [idx=" + segmentIdx + ", await=" + walSegments.keySet() + "]");

        if (stopped)
            return;

        Map<Integer, Set<Integer>> grpKeys = walSegments.remove(segmentIdx);

        synchronized (metaStorageMux) {
            if (grpKeys != null) {
                for (Map.Entry<Integer, Set<Integer>> entry : grpKeys.entrySet()) {
                    int grpId = entry.getKey();
                    Set<Integer> keyIds = entry.getValue();

                    ctx.cache().context().database().checkpointReadLock();

                    try {
                        metaStorage.write(REENCRYPTED_WAL_SEGMENTS, (Serializable)walSegments);

                        if (reencryptGroups.contains(grpId))
                            continue;

                        boolean rmv = grpEncKeys.get(grpId).keySet().removeAll(keyIds);

                        assert rmv;

                        metaStorage.write(ENCRYPTION_KEYS_PREFIX + grpId, groupKeysEncrypted(grpId));

                        log.info("Previous encryption keys were removed");
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to remove encryption keys from metastore.", e);
                    }
                    finally {
                        ctx.cache().context().database().checkpointReadUnlock();
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        try {
            // There is no need to set master key in case of recovery, as it is already relevant.
            if (!restoredFromWAL) {
                String masterKeyName = (String)metastorage.read(MASTER_KEY_NAME_PREFIX);

                if (masterKeyName != null) {
                    log.info("Master key name loaded from metastrore [masterKeyName=" + masterKeyName + ']');

                    getSpi().setMasterKeyName(masterKeyName);
                }
            }

            metastorage.iterate(ENCRYPTION_KEYS_PREFIX, (key, val) -> {
                try {
                    Integer grpId = Integer.valueOf(key.replace(ENCRYPTION_KEYS_PREFIX, ""));

                    if (!grpEncKeys.containsKey(grpId)) {
                        Map<Integer, byte[]> encGrpKey = (Map<Integer, byte[]>)val;

                        Map<Integer, Serializable> keyMap = new ConcurrentHashMap<>(encGrpKey.size());

                        for (Map.Entry<Integer, byte[]> e : encGrpKey.entrySet())
                            keyMap.put(e.getKey(), getSpi().decryptKey(e.getValue()));

                        grpEncKeys.put(grpId, keyMap);
                    }

                    Byte activeKey = (Byte)metastorage.read(ENCRYPTION_ACTIVE_KEY_PREFIX + grpId);

                    if (activeKey == null)
                        activeKey = 0;

                    String sysProp = IGNITE_ACTIVE_KEY_ID_FOR_GROUP + grpId;

                    int activeGrpKey =
                        IgniteSystemProperties.getInteger(sysProp, Integer.MIN_VALUE);

                    if (activeGrpKey != Integer.MIN_VALUE) {
                        if (activeGrpKey == activeKey) {
                            log.warning("Restored group key idenitifier equals to identifier of system property " +
                                sysProp + ". It is strongly recommended to remove this " +
                                "system property [keyId=" + activeGrpKey + ']');
                        }
                        else {
                            Map<Integer, Serializable> keyMap = grpEncKeys.get(grpId);

                            if (keyMap != null && keyMap.containsKey(activeGrpKey)) {
                                if (activeGrpKey < (activeKey & 0xff)) {
                                    log.warning("Group key idenitifier that was set using system property " +
                                        sysProp + " may be incorrect, if node cannot join cluster, remove this " +
                                        "property [curr=" + (activeKey & 0xff) + ", new=" + activeGrpKey + "]");
                                }

                                if (log.isInfoEnabled()) {
                                    log.info("Group key idenitifier is set using system property " + sysProp +
                                        " [prev=" + (activeKey & 0xff) + ", new=" + activeGrpKey + "]");
                                }

                                reencryptGroupsForced.add(grpId);

                                activeKey = (byte)activeGrpKey;
                            } else {
                                log.error("Incorrect value was set for system property " + sysProp +
                                    " [value=" + activeGrpKey + ", available=" + keyMap.keySet() + "]");
                            }
                        }
                    }

                    grpEncActiveIds.put(grpId, activeKey & 0xff);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }, true);

            // Try to read keys in previous format.
            if (grpEncKeys.isEmpty()) {
                final Map<Integer, Serializable> singleGrpEncKeys = new HashMap<>();

                metastorage.iterate(ENCRYPTION_KEY_PREFIX, (key, val) -> {
                    Integer grpId = Integer.valueOf(key.replace(ENCRYPTION_KEY_PREFIX, ""));

                    byte[] encGrpKey = (byte[])val;

                    singleGrpEncKeys.computeIfAbsent(grpId, k -> getSpi().decryptKey(encGrpKey));
                }, true);

                for (Map.Entry<Integer, Serializable> entry : singleGrpEncKeys.entrySet()) {
                    Map<Integer, Serializable> keysMap = new ConcurrentHashMap<>(1);
                    keysMap.put(0, entry.getValue());

                    grpEncKeys.put(entry.getKey(), keysMap);

                    grpEncActiveIds.put(entry.getKey(), 0);
                }
            }

            Map<Long, Map<Integer, Set<Integer>>> map =
                (Map<Long, Map<Integer, Set<Integer>>>)metastorage.read(REENCRYPTED_WAL_SEGMENTS);

            if (map != null)
                walSegments.putAll(map);

            if (!grpEncKeys.isEmpty()) {
                U.quietAndInfo(log, "Encryption keys loaded from metastore. [grps=" +
                    F.concat(grpEncKeys.keySet(), ",") + ", masterKeyName=" + getSpi().getMasterKeyName() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read encryption keys state.", e);
        }

        String newMasterKeyName = IgniteSystemProperties.getString(IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP);

        if (newMasterKeyName != null) {
            if (newMasterKeyName.equals(getSpi().getMasterKeyName())) {
                log.info("Restored master key name equals to name from system property " +
                    IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP + ". This system property will be ignored and " +
                    "recommended to remove [masterKeyName=" + newMasterKeyName + ']');

                return;
            }

            recoveryMasterKeyName = true;

            log.info("System property " + IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP + " is set. Master key " +
                "will be changed locally and group keys will be re-encrypted before join to cluster. Result will " +
                "be saved to MetaStore on activation process. [masterKeyName=" + newMasterKeyName + ']');

            getSpi().setMasterKeyName(newMasterKeyName);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metaStorage) throws IgniteCheckedException {
        synchronized (metaStorageMux) {
            this.metaStorage = metaStorage;

            writeToMetaStoreEnabled = true;

            if (recoveryMasterKeyName)
                writeKeysToWal();

            writeKeysToMetaStore(restoredFromWAL || recoveryMasterKeyName);

            restoredFromWAL = false;

            recoveryMasterKeyName = false;
        }

        // Key ID for specifid groups was changed manually, we need schedule re-encryption with the new key.
        Iterator<Integer> itr = reencryptGroupsForced.iterator();

        while (itr.hasNext()) {
            int grpId = itr.next();

            storeEncryptedPagesCount(grpId);

            markForReencryption(grpId);

            itr.remove();
        }

        startReencryption(reencryptGroups, true);
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

    public void markForReencryption(int grpId) {
        reencryptGroups.add(grpId);
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

    private List<T2<Integer, Integer>> storeEncryptedPagesCount(int grpId) throws IgniteCheckedException {
        List<T2<Integer, Integer>> offsets = new ArrayList<>();

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        FilePageStoreManager mgr = (FilePageStoreManager)ctx.cache().context().pageStore();

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            GridDhtLocalPartition part = grp.topology().localPartition(p);

            if (part == null)
                continue;

            PageStore pageStore = mgr.getStore(grpId, part.id());

            int pagesCnt = pageStore.pages();

            pageStore.encryptPageCount(pagesCnt);
            pageStore.encryptPageIndex(0);

            offsets.add(new T2<>(part.id(), pagesCnt));

            log.info(">xxx> store encr offset " + grp.cacheOrGroupName() + " p=" + part.id() + " total=" + pagesCnt + ", ");
        }

        PageStore pageStore = mgr.getStore(grpId, INDEX_PARTITION);

        int pagesCnt = pageStore.pages();

        offsets.add(new T2<>(INDEX_PARTITION, pagesCnt));

        pageStore.encryptPageCount(pagesCnt);
        pageStore.encryptPageIndex(0);

        return offsets;
    }

    /**
     * @param grpIds Cache group IDs.
     * @param skipDirty Skip dirty pages.
     * @throws IgniteCheckedException If failed.
     */
    private void startReencryption(Collection<Integer> grpIds, boolean skipDirty) throws IgniteCheckedException {
        for (int grpId : grpIds) {
            IgniteInternalFuture<?> fut = reencryption.schedule(grpId, skipDirty);

            fut.listen(f -> {
                try {
                    f.get();

                    cleanupKeys(grpId);
                }
                catch (IgniteFutureCancelledCheckedException e) {
                    log.warning("Reencryption cancelled [grp=" + grpId + "]");
                }
                catch (IgniteCheckedException e) {
                    log.warning("Reencryption failed [grp=" + grpId + "]", e);
                }
            });
        }
    }

    /**
     * @param grpId Cache group ID.
     * @throws IgniteCheckedException If failed.
     */
    private void cleanupKeys(int grpId) throws IgniteCheckedException {
        int activeKey = grpEncActiveIds.get(grpId);

        Set<Integer> rmvKeys = new HashSet<>(grpEncKeys.get(grpId).keySet());

        rmvKeys.remove(activeKey);

        for (Map<Integer, Set<Integer>> map : walSegments.values()) {
            Set<Integer> grpKeepKeys = map.get(grpId);

            rmvKeys.removeAll(grpKeepKeys);
        }

        boolean changed = grpEncKeys.get(grpId).keySet().removeAll(rmvKeys);

        ctx.cache().context().database().checkpointReadLock();

        try {
            reencryptGroups.remove(grpId);

            if (changed) {
                metaStorage.write(ENCRYPTION_KEYS_PREFIX + grpId, groupKeysEncrypted(grpId));

                if (log.isInfoEnabled())
                    log.info("Previous encryption keys were removed [ids=" + rmvKeys + "]");
            }
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
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

        for (Map.Entry<Integer, Map<Integer, Serializable>> entry : grpEncKeys.entrySet()) {
            if (!writeAll && metaStorage.read(ENCRYPTION_KEYS_PREFIX + entry.getKey()) != null)
                continue;

            // todo
            Integer keyId = grpEncActiveIds.get(entry.getKey());

            assert keyId != null;

            writeToMetaStore(entry.getKey(), true, true, true);
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
     * Writes cache group encryption keys to metastore.
     *
     * @param grpId Cache group ID.
     */
    private void writeToMetaStore(int grpId) throws IgniteCheckedException {
        writeToMetaStore(grpId, true, true, true);
    }

    /**
     * Writes cache group encryption keys to metastore.
     *
     * @param grpId Cache group ID.
     * @param newKeys Write existing encryption keys.
     * @param activeKeys Write active key identifier.
     * @param walIdxs Write WAL segment indexes.
     */
    private void writeToMetaStore(int grpId, boolean newKeys, boolean activeKeys, boolean walIdxs) throws IgniteCheckedException {
        if (metaStorage == null || !writeToMetaStoreEnabled)
            return;

        ctx.cache().context().database().checkpointReadLock();

        try {
            if (newKeys)
                metaStorage.write(ENCRYPTION_KEYS_PREFIX + grpId, groupKeysEncrypted(grpId));

            if (activeKeys)
                metaStorage.write(ENCRYPTION_ACTIVE_KEY_PREFIX + grpId, (byte)grpEncActiveIds.get(grpId).intValue());

            if (walIdxs)
                metaStorage.write(REENCRYPTED_WAL_SEGMENTS, (Serializable)this.walSegments);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private HashMap<Integer, byte[]> groupKeysEncrypted(int grpId) {
        Map<Integer, Serializable> keys = grpEncKeys.get(grpId);
        HashMap<Integer, byte[]> keysEncrypted = new HashMap<>();

        assert !F.isEmpty(keys);

        for (Map.Entry<Integer, Serializable> entry : keys.entrySet())
            keysEncrypted.put(entry.getKey(), getSpi().encryptKey(entry.getValue()));

        return keysEncrypted;
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
     * @return Local encryption keys.
     */
    @Nullable private HashMap<Integer, T2<Integer, byte[]>> knownEncryptionKeys() {
        if (F.isEmpty(grpEncKeys))
            return null;

        HashMap<Integer, T2<Integer, byte[]>> knownKeys = new HashMap<>();

        for (Map.Entry<Integer, Map<Integer, Serializable>> entry : grpEncKeys.entrySet()) {
            int grpId = entry.getKey();
            Map<Integer, Serializable> grpKeys = entry.getValue();
            int activeId = grpEncActiveIds.get(grpId);

            knownKeys.put(grpId, new T2<>(activeId, getSpi().encryptKey(grpKeys.get(activeId))));
        }

        return knownKeys;
    }

    /**
     * Generates required count of encryption keys.
     *
     * @param keyCnt Keys count.
     * @return Tuple of collection with newly generated encryption keys and master key digest.
     */
    private T2<Collection<byte[]>, byte[]> createKeys(int keyCnt) {
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
        log.info("Start master key change [masterKeyName=" + name + ']');

        masterKeyChangeLock.writeLock().lock();

        try {
            getSpi().setMasterKeyName(name);

            ctx.cache().context().database().checkpointReadLock();

            try {
                writeKeysToWal();

                synchronized (metaStorageMux) {
                    assert writeToMetaStoreEnabled;

                    writeKeysToMetaStore(true);
                }
            } finally {
                ctx.cache().context().database().checkpointReadUnlock();
            }

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
        List<T3<Integer, Byte, byte[]>> reencryptedKeys = new ArrayList<>();

        for (Map.Entry<Integer, Map<Integer, Serializable>> entry : grpEncKeys.entrySet()) {
            for (Map.Entry<Integer, Serializable> e : entry.getValue().entrySet())
                reencryptedKeys.add(new T3<>(entry.getKey(), (byte)e.getKey().intValue(), getSpi().encryptKey(e.getValue())));
        }

        MasterKeyChangeRecord rec = new MasterKeyChangeRecord(getSpi().getMasterKeyName(), reencryptedKeys);

        WALPointer ptr = ctx.cache().context().wal().log(rec);

        assert ptr != null;
    }

    /**
     * Apply keys from WAL record during the recovery phase.
     *
     * @param rec Record.
     */
    public void applyKeys(MasterKeyChangeRecord rec) {
        assert !writeToMetaStoreEnabled && !ctx.state().clusterState().active();

        log.info("Master key name loaded from WAL [masterKeyName=" + rec.getMasterKeyName() + ']');

        try {
            getSpi().setMasterKeyName(rec.getMasterKeyName());

            for (T3<Integer, Byte, byte[]> entry : rec.getGrpKeys()) {
                int grpId = entry.get1();
                int keyId = entry.get2() & 0xff;
                byte[] key = entry.get3();

                grpEncKeys.computeIfAbsent(grpId, m -> new ConcurrentHashMap<>()).put(keyId, getSpi().decryptKey(key));
            }

            restoredFromWAL = true;
        } catch (IgniteSpiException e) {
            log.warning("Unable to apply group keys from WAL record [masterKeyName=" + rec.getMasterKeyName() + ']', e);
        }
    }

    /**
     * Restart encryption of existing pages with the new key.
     *
     * @param rec Encryption status record.
     */
    public void applyEncryptionStatus(EncryptionStatusRecord rec) throws IgniteCheckedException {
        assert !writeToMetaStoreEnabled && !ctx.state().clusterState().active();

        FilePageStoreManager mgr = (FilePageStoreManager)ctx.cache().context().pageStore();

        for (Map.Entry<Integer, List<T2<Integer, Integer>>> entry : rec.groupsStatus().entrySet()) {
            int grpId = entry.getKey();

            for (T2<Integer, Integer> state : entry.getValue()) {
                int partId = state.getKey();

                PageStore pageStore = mgr.getStore(grpId, partId);

                pageStore.encryptPageCount(state.getValue());
            }

            markForReencryption(grpId);
        }

        if (log.isInfoEnabled())
            log.info("Re-encryption status applied [grpIds=" + rec.groupsStatus().keySet() + "]");
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

        if (grpKeyChangeProc.inProgress()) {
            return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. " +
                "Cache encryption key change is in progress."));
        }

        masterKeyChangeRequest = req;

        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        try {
            String masterKeyName = decryptKeyName(req.encKeyName());

            if (masterKeyName.equals(getMasterKeyName()))
                throw new IgniteException("Master key change was rejected. New name equal to the current.");

            byte[] digest = masterKeyDigest(masterKeyName);

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
        else if (isCoordinator())
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
            if (completeKeyChangeFuture(reqId, err, masterKeyChangeFut))
                masterKeyChangeFut = null;
        }
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeKeyChangeFuture(UUID reqId, Map<UUID, Exception> err, KeyChangeFuture fut) {
        boolean isInitiator = fut != null && fut.id().equals(reqId);

        if (!isInitiator || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    /**
     * @param msg Error message.
     */
    private void cancelFutures(String msg) {
        for (GenerateEncryptionKeyFuture fut : genEncKeyFuts.values())
            fut.onDone(new IgniteFutureCancelledException(msg));

        GridFutureAdapter masterChangeFut = masterKeyChangeFut;

        if (masterChangeFut != null && !masterChangeFut.isDone())
            masterChangeFut.onDone(new IgniteFutureCancelledException(msg));

        GridFutureAdapter keyChangeFut = grpKeyChangeProc.grpKeyChangeFut;

        if (keyChangeFut != null && !keyChangeFut.isDone())
            keyChangeFut.onDone(new IgniteFutureCancelledException(msg));
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    private boolean isCoordinator() {
        DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            return ((TcpDiscoverySpi)spi).isLocalNodeCoordinator();
        else {
            ClusterNode crd = U.oldest(ctx.discovery().aliveServerNodes(), null);

            return crd != null && F.eq(ctx.localNodeId(), crd.id());
        }
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
    public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /**
     * @param c Callable to run with master key change read lock.
     * @return Computed result.
     */
    private <T> T withMasterKeyChangeReadLock(Callable<T> c) {
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

    /**
     * Two phase distributed process, that performs encryption group key rotation.
     */
    private class GroupKeyChangeProcess {
        /** Group key change future. */
        private volatile GroupKeyChangeFuture grpKeyChangeFut;

        /** Group key change request. */
        private volatile ChangeCacheEncryptionRequest grpKeyChangeReq;

        /** Cache group encyption key change prepare phase. */
        DistributedProcess<ChangeCacheEncryptionRequest, EmptyResult> prepareGKChangeProc =
            new DistributedProcess<>(ctx, GROUP_KEY_CHANGE_PREPARE, this::prepare, this::finishPrepare,
                (id, req) -> new InitMessage<>(id, GROUP_KEY_CHANGE_PREPARE, req), true);

        /** Cache group encyption key change perform phase. */
        DistributedProcess<ChangeCacheEncryptionRequest, EmptyResult> performGKChangeProc =
            new DistributedProcess<>(ctx, GROUP_KEY_CHANGE_FINISH, this::perform, this::finishPerform);

        /**
         * @return {@code True} if operation is still in progress.
         */
        public boolean inProgress() {
            return grpKeyChangeReq != null;
        }

        /**
         * @return {@code True} if operation is not finished.
         */
        public boolean finished() {
            GroupKeyChangeFuture fut = grpKeyChangeFut;

            return fut == null || fut.isDone();
        }

        /**
         * @param grpIds Cache group IDs.
         */
        private void doChangeGroupKeys(int[] grpIds) throws IgniteCheckedException {
            Map<Integer, List<T2<Integer, Integer>>> encryptionStatus = new HashMap<>();

            ctx.cache().context().database().checkpointReadLock();

            try {
                for (int grpId : grpIds) {
                    List<T2<Integer, Integer>> offsets = storeEncryptedPagesCount(grpId);

                    encryptionStatus.put(grpId, offsets);

                    markForReencryption(grpId);
                }
            } finally {
                ctx.cache().context().database().checkpointReadUnlock();
            }

            WALPointer ptr = ctx.cache().context().wal().log(new EncryptionStatusRecord(encryptionStatus));

            if (ptr != null)
                ctx.cache().context().wal().flush(ptr, false);

            startReencryption(Arrays.stream(grpIds).boxed().collect(Collectors.toList()), false);
        }

        /**
         * @param req Request.
         */
        public IgniteFuture<Void> start(ChangeCacheEncryptionRequest req) {
            if (disconnected) {
                return new IgniteFinishedFutureImpl<>(new IgniteClientDisconnectedException(
                    ctx.cluster().clientReconnectFuture(),
                    "Group key change was rejected. Client node disconnected."));
            }

            if (stopped) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Group key change was rejected. " +
                    "Node is stopping."));
            }

            if (!finished()) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Group key change was rejected. " +
                    "The previous change was not completed."));
            }

            if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone()) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Group key change was rejected. " +
                    "The previous master key change was not completed."));
            }

            grpKeyChangeFut = new GroupKeyChangeFuture(req);

            prepareGKChangeProc.start(req.requestId(), req);

            return new IgniteFutureImpl<>(grpKeyChangeFut);
        }

        /**
         * Validates existing keys and adds new encryption key as inactive.
         *
         * @param req Request.
         * @return Result future.
         */
        private IgniteInternalFuture<EmptyResult> prepare(ChangeCacheEncryptionRequest req) {
            if (inProgress()) {
                return new GridFinishedFuture<>(new IgniteException("Group key change was rejected. " +
                    "The previous change was not completed."));
            }

            if (isMasterKeyChangeInProgress()) {
                return new GridFinishedFuture<>(new IgniteException("Group key change was rejected. " +
                    "The previous master key change was not completed."));
            }

            grpKeyChangeReq = req;

            if (ctx.clientNode())
                return new GridFinishedFuture<>();

            try {
                int n = 0;

                for (int grpId : req.groups()) {
                    if (!encryptionTask(grpId).isDone())
                        return new GridFinishedFuture<>(
                            new IgniteException("Reencryption is in progress [grpId=" + grpId + "]"));

                    int newKeyId = req.keyIdentifiers()[n] & 0xff;

                    if (grpEncKeys.get(grpId).containsKey(newKeyId) && grpEncActiveIds.get(grpId) >= newKeyId) {
                        Set<Long> walIdxs = new TreeSet<>();

                        for (Map.Entry<Long, Map<Integer, Set<Integer>>> entry : walSegments.entrySet()) {
                            Set<Integer> keys = entry.getValue().get(grpId);

                            if (keys != null && keys.contains(newKeyId))
                                walIdxs.add(entry.getKey());
                        }

                        return new GridFinishedFuture<>(new IgniteException("Cannot add new key identifier - it's " +
                            "already present, probably there existing wal logs that encrypted with this key [" +
                            "grp=" + grpId + ", keyId=" + newKeyId + " walSegments=" + walIdxs + "]."));
                    }

                    addNewKeys(req.groups(), req.keys(), req.keyIdentifiers(), false);

                    ++n;
                }
            }
            catch (Exception e) {
                return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected [nodeId=" +
                    ctx.localNodeId() + ']', e));
            }

            return new GridFinishedFuture<>(new EmptyResult());
        }

        /**
         * Starts group key change if there are no errors.
         *
         * @param id Request id.
         * @param res Results.
         * @param err Errors.
         */
        private void finishPrepare(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
            if (!err.isEmpty()) {
                if (grpKeyChangeReq != null && grpKeyChangeReq.requestId().equals(id))
                    grpKeyChangeReq = null;

                synchronized (opsMux) {
                    completeKeyChangeFuture(id, err, grpKeyChangeFut);

                    grpKeyChangeFut = null;
                }
            }
            else if (isCoordinator())
                performGKChangeProc.start(id, grpKeyChangeReq);
        }

        /**
         * Sets new encrpytion key as active (for writing) and starts background re-encryption.
         *
         * @param req Request.
         * @return Result future.
         */
        private IgniteInternalFuture<EmptyResult> perform(ChangeCacheEncryptionRequest req) {
            if (grpKeyChangeReq == null || !grpKeyChangeReq.equals(req))
                return new GridFinishedFuture<>(new IgniteException("Unknown group key change was rejected."));

            if (!ctx.state().clusterState().active()) {
                grpKeyChangeReq = null;

                return new GridFinishedFuture<>(new IgniteException("Group key change was rejected. " +
                    "The cluster is inactive."));
            }

            try {
                if (!ctx.clientNode()) {
                    addNewKeys(req.groups(), req.keys(), req.keyIdentifiers(), true);

                    doChangeGroupKeys(req.groups());
                }

                grpKeyChangeReq = null;

                return new GridFinishedFuture<>(new EmptyResult());
            } catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }

        /**
         * Finishes cache encryption key rotation.
         *
         * @param id Request id.
         * @param res Results.
         * @param err Errors.
         */
        private void finishPerform(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
            synchronized (opsMux) {
                GroupKeyChangeFuture fut = grpKeyChangeFut;

                if (completeKeyChangeFuture(id, err, fut)) {
                    grpKeyChangeFut = null;

                    ChangeCacheEncryptionRequest req = fut.request();

                    AffinityTopologyVersion initVer = req.topologyVersion();

                    if (initVer.topologyVersion() != ctx.discovery().topologyVersion()) {
                        DiscoCache discoCache = ctx.discovery().discoCache();

                        Set<BaselineNode> bltNodes = new HashSet<>(discoCache.baselineNodes());

                        bltNodes.removeAll(discoCache.aliveBaselineNodes());

                        int[] keyIds = new int[req.keyIdentifiers().length];

                        for (int i = 0; i < req.keyIdentifiers().length; i++)
                            keyIds[i] = req.keyIdentifiers()[i] & 0xff;

                        log.warning("Cache group key rotation may be incomplete on missed baseline nodes, " +
                            "this node(s) should be preconfigured to re-join the cluster with the existing data [" +
                            "nodes=" + F.viewReadOnly(bltNodes, BaselineNode::consistentId) +
                            ", grps=" + Arrays.toString(req.groups()) +
                            ", keyIds=" + Arrays.toString(keyIds) + "]");
                    }
                }
            }
        }

        /**
         * @param groups Cache group identifiers.
         * @param keys Cache group encryption keys.
         * @param keyIds Cache group encryption key identifiers.
         * @param active {@code True} to set new key for writing.
         */
        void addNewKeys(int[] groups, byte[][] keys, byte[] keyIds, boolean active) throws IgniteCheckedException {
            for (int i = 0; i < groups.length; i++) {
                int grpId = groups[i];

                int keyId = keyIds[i] & 0xff;

                replaceKey(grpId, keys[i], keyId, !active, active);

                if (log.isInfoEnabled()) {
                    log.info("New encryption key for cache group added [" +
                        "grp=" + grpId + ", id=" + keyId + ", active=" + active + "]");
                }
            }
        }
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
    private static class EmptyResult implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /** */
    public static class NodeEncryptionKeys implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        NodeEncryptionKeys(
            HashMap<Integer, T2<Integer, byte[]>> knownKeysWithIds,
            Map<Integer, byte[]> newKeys,
            byte[] masterKeyDigest
        ) {
            this.newKeys = newKeys;
            this.masterKeyDigest = masterKeyDigest;

            if (F.isEmpty(knownKeysWithIds))
                return;

            // For backward compatibilty store key identifiers into separate field.
            knownKeys = U.newHashMap(knownKeysWithIds.size());
            knownKeyIds = U.newHashMap(knownKeysWithIds.size());

            for (Map.Entry<Integer, T2<Integer, byte[]>> entry : knownKeysWithIds.entrySet()) {
                knownKeys.put(entry.getKey(), entry.getValue().get2());
                knownKeyIds.put(entry.getKey(), (byte)entry.getValue().get1().intValue());
            }
        }

        /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node. */
        Map<Integer, byte[]> knownKeys;

        /**  New keys i.e. keys for a local statically configured caches. */
        Map<Integer, byte[]> newKeys;

        /** Master key digest. */
        byte[] masterKeyDigest;

        /** Identifiers for known keys. */
        Map<Integer, Byte> knownKeyIds;
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
    private static class KeyChangeFuture extends GridFutureAdapter<Void> {
        /** Request ID. */
        private final UUID id;

        /** @param id Request ID. */
        private KeyChangeFuture(UUID id) {
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

    /** Group key change future. */
    private static class GroupKeyChangeFuture extends KeyChangeFuture {
        /** Request. */
        private final ChangeCacheEncryptionRequest req;

        /**
         * @param req Request.
         */
        private GroupKeyChangeFuture(ChangeCacheEncryptionRequest req) {
            super(req.requestId());

            this.req = req;
        }

        /** @return Topology version. */
        public ChangeCacheEncryptionRequest request() {
            return req;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GroupKeyChangeFuture.class, this);
        }
    }
}
