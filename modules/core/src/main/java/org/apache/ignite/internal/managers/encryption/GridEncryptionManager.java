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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridPlainClosure;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ENCRYPTION_MGR;
import static org.apache.ignite.internal.GridTopic.TOPIC_GEN_ENC_KEY;
import static org.apache.ignite.internal.IgniteFeatures.MASTER_KEY_CHANGE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_ENCRYPTION_MASTER_KEY_DIGEST;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.util.distributed.DistributedProcesses.MASTER_KEY_CHANGE_FINISH;
import static org.apache.ignite.internal.util.distributed.DistributedProcesses.MASTER_KEY_CHANGE_PREPARE;

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
 *
 * @see GridCacheProcessor#generateEncryptionKeysAndStartCacheAfter(int, GridPlainClosure)
 */
public class GridEncryptionManager extends GridManagerAdapter<EncryptionSpi> implements MetastorageLifecycleListener,
    IgniteChangeGlobalStateSupport, IgniteEncryption {
    /**
     * Cache encryption introduced in this Ignite version.
     */
    private static final IgniteProductVersion CACHE_ENCRYPTION_SINCE = IgniteProductVersion.fromString("2.7.0");

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

    /** {@code True} if need to write all keys (and rewrite existing) when metastore will ready for write. */
    private volatile boolean forceWriteAllKeysToMetaStore;

    /** Prefix for a encryption group key in meta store. */
    public static final String ENCRYPTION_KEY_PREFIX = "grp-encryption-key-";

    /** Prefix for a master key name. */
    public static final String MASTER_KEY_NAME_PREFIX = "encryption-master-key-name";

    /** Group encryption keys. */
    private final ConcurrentHashMap<Integer, Serializable> grpEncKeys = new ConcurrentHashMap<>();

    /** Pending generate encryption key futures. */
    private ConcurrentMap<IgniteUuid, GenerateEncryptionKeyFuture> genEncKeyFuts = new ConcurrentHashMap<>();

    /** Metastorage. */
    private volatile ReadWriteMetastorage metaStorage;

    /** I/O message listener. */
    private GridMessageListener ioLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Master key change future. */
    private volatile MasterKeyChangeFuture masterKeyChangeFut;

    /** Pending master key request or {@code null} if there is no master key change process. */
    private volatile PendingMasterKey pendingMasterKey;

    /** Digest of last changed master key or {@code null} if master key was not changed. */
    private volatile byte[] lastChangedMasterKeyDigest;

    /**
     * Master key change prepare process. Checks that all server nodes have the same new master key and then starts
     * finish process.
     */
    private DistributedProcess<PendingMasterKey, MasterKeyChangeResult> prepareProc;

    /** Master key change finish process. Changes master key. */
    private DistributedProcess<PendingMasterKey, MasterKeyChangeResult> finishProc;

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

        if (!ctx.clientNode() && getSpi().masterKeyDigest() != null)
            ctx.addNodeAttribute(ATTR_ENCRYPTION_MASTER_KEY_DIGEST, getSpi().masterKeyDigest());

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

                    byte[] masterKeyDigest;

                    masterKeyChangeLock.readLock().lock();

                    try {
                        for (int i = 0; i < req.keyCount(); i++)
                            encKeys.add(getSpi().encryptKey(getSpi().create()));

                        // We should send the master key digest that encrypted group keys because the response can be
                        // processed after the possible master key change.
                        masterKeyDigest = getSpi().masterKeyDigest();
                    }
                    finally {
                        masterKeyChangeLock.readLock().unlock();
                    }

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

        prepareProc = new DistributedProcess<>(ctx, MASTER_KEY_CHANGE_PREPARE,
            (req) -> {
                if (pendingMasterKey != null) {
                    return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. " +
                        "Previous change was not completed."));
                }

                pendingMasterKey = req;

                if (ctx.clientNode())
                    return new GridFinishedFuture<>();

                try {
                    byte[] digest = masterKeyDigest(decryptKeyName(req.encKeyName()));

                    if (!Arrays.equals(req.digest, digest)) {
                        return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. Master " +
                            "key digest consistency check failed. Make sure that a new master key is the same at " +
                            "all server nodes [nodeId=" + ctx.localNodeId() + ']'));
                    }
                }
                catch (Exception e) {
                    return new GridFinishedFuture<>(new IgniteException("Master key change was rejected [nodeId=" +
                        ctx.localNodeId() + ']', e));
                }

                return new GridFinishedFuture<>(new MasterKeyChangeResult());
            },
            (id, res, err) -> {
                if (!err.isEmpty())
                    finishMasterKeyChange(id, err);
                else if (isCoordinator())
                    finishProc.start(id, pendingMasterKey);
            }
        );

        finishProc = new DistributedProcess<>(ctx, MASTER_KEY_CHANGE_FINISH,
            (req) -> {
                if (pendingMasterKey == null || !pendingMasterKey.equals(req))
                    return new GridFinishedFuture<>(new IgniteException("Unknown master key change was rejected."));

                boolean active = ctx.state().clusterState().active();

                if (!active) {
                    return new GridFinishedFuture<>(new IgniteException("Master key change was rejected. " +
                        "The cluster is inactive."));
                }

                if (!ctx.clientNode())
                    changeMasterKeyAndReencryptGroupKeys(decryptKeyName(req.encKeyName()));

                lastChangedMasterKeyDigest = req.digest();

                return new GridFinishedFuture<>(new MasterKeyChangeResult());
            },
            (id, res, err) -> finishMasterKeyChange(id, err)
        );
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
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

            pendingMasterKey = null;

            lastChangedMasterKeyDigest = null;

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
            HashMap<Integer, byte[]> knownEncKeys = knownEncryptionKeys();

            //Generated(not saved!) keys for a new caches.
            //Configured statically in config, but doesn't stored on the disk.
            HashMap<Integer, byte[]> newEncKeys =
                newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

            if (newEncKeys == null)
                return;

            //We can store keys to the disk, because we are on a coordinator.
            for (Map.Entry<Integer, byte[]> entry : newEncKeys.entrySet()) {
                groupKey(entry.getKey(), entry.getValue());

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
                "Master key change in progress! Node join is rejected. [node=" + node.id() + "]",
                "Master key change in progress! Node join is rejected.");
        }

        if (node.isClient() || node.isDaemon())
            return null;

        res = validateNode(node);

        if (res != null)
            return res;

        NodeEncryptionKeys nodeEncKeys;

        if (!discoData.hasJoiningNodeData() ||
            (nodeEncKeys = (NodeEncryptionKeys)discoData.joiningNodeData()) == null) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Joining node doesn't have encryption data [node=" + node.id() + "]",
                "Joining node doesn't have encryption data.");
        }

        if (!Arrays.equals(getSpi().masterKeyDigest(), nodeEncKeys.masterKeyDigest)) {
            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Master key digest differs! Node join is rejected. [node=" + node.id() + "]",
                "Master key digest differs! Node join is rejected.");
        }

        if (F.isEmpty(nodeEncKeys.knownKeys)) {
            U.quietAndInfo(log, "Joining node doesn't have stored group keys [node=" + node.id() + "]");

            return null;
        }

        for (Map.Entry<Integer, byte[]> entry : nodeEncKeys.knownKeys.entrySet()) {
            Serializable locEncKey = grpEncKeys.get(entry.getKey());

            if (locEncKey == null)
                continue;

            Serializable rmtKey = getSpi().decryptKey(entry.getValue());

            if (F.eq(locEncKey, rmtKey))
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

        HashMap<Integer, byte[]> knownEncKeys = knownEncryptionKeys();

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

                groupKey(entry.getKey(), entry.getValue());
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

        HashMap<Integer, byte[]> knownEncKeys = knownEncryptionKeys();

        HashMap<Integer, byte[]> newKeys =
            newEncryptionKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

        if (knownEncKeys == null)
            knownEncKeys = newKeys;
        else if (newKeys != null) {
            for (Map.Entry<Integer, byte[]> entry : newKeys.entrySet()) {
                byte[] old = knownEncKeys.putIfAbsent(entry.getKey(), entry.getValue());

                assert old == null;
            }
        }

        dataBag.addGridCommonData(ENCRYPTION_MGR.ordinal(), knownEncKeys);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        if (ctx.clientNode())
            return;

        Map<Integer, byte[]> encKeysFromCluster = (Map<Integer, byte[]>)data.commonData();

        if (F.isEmpty(encKeysFromCluster))
            return;

        for (Map.Entry<Integer, byte[]> entry : encKeysFromCluster.entrySet()) {
            if (groupKey(entry.getKey()) == null) {
                U.quietAndInfo(log, "Store group key received from coordinator [grp=" + entry.getKey() + "]");

                groupKey(entry.getKey(), entry.getValue());
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
     * @param grpId Group id.
     * @return Group encryption key.
     */
    @Nullable public Serializable groupKey(int grpId) {
        if (grpEncKeys.isEmpty())
            return null;

        return grpEncKeys.get(grpId);
    }

    /**
     * Store group encryption key.
     *
     * @param grpId Group id.
     * @param encGrpKey Encrypted group key.
     */
    public void groupKey(int grpId, byte[] encGrpKey) {
        assert !grpEncKeys.containsKey(grpId);

        Serializable encKey;

        masterKeyChangeLock.readLock().lock();

        try {
            encKey = getSpi().decryptKey(encGrpKey);
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }

        synchronized (metaStorageMux) {
            if (log.isDebugEnabled())
                log.debug("Key added. [grp=" + grpId + "]");

            grpEncKeys.put(grpId, encKey);

            writeToMetaStore(grpId, encGrpKey);
        }
    }

    /** @return Digest of last changed master key or {@code null} if master key was not changed. */
    public byte[] lastChangedMasterKeyDigest() {
        return lastChangedMasterKeyDigest;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> changeMasterKey(String masterKeyName) {
        if (ctx.clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        checkMasterKeyChangeSupported();

        if (masterKeyName.equals(getMasterKeyName()))
            return new IgniteFutureImpl<>(new GridFinishedFuture<>());

        byte[] digest = masterKeyDigest(masterKeyName);

        PendingMasterKey request = new PendingMasterKey(UUID.randomUUID(), encryptKeyName(masterKeyName), digest);

        synchronized (opsMux) {
            checkState();

            if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone())
                throw new IgniteException("Master key change is in progress.");

            masterKeyChangeFut = new MasterKeyChangeFuture(request.requestId());
        }

        prepareProc.start(request.requestId(), request);

        return new IgniteFutureImpl<>(masterKeyChangeFut);
    }

    /** {@inheritDoc} */
    @Override public String getMasterKeyName() {
        if (ctx.clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        checkMasterKeyChangeSupported();

        masterKeyChangeLock.readLock().lock();

        try {
            return getSpi().getMasterKeyName();
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
        masterKeyChangeLock.readLock().lock();

        try {
            Serializable key = getSpi().create();

            byte[] encKey = getSpi().encryptKey(key);

            byte[] serKeyName = U.toBytes(keyName);

            ByteBuffer res = ByteBuffer.allocate(/*Encrypted key length*/4 + encKey.length +
                getSpi().encryptedSize(serKeyName.length));

            res.putInt(encKey.length);
            res.put(encKey);

            getSpi().encrypt(ByteBuffer.wrap(serKeyName), key, res);

            return res.array();
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
    }

    /**
     * @param data Byte array with encrypted a master key name.
     * @return Decrypted master key name.
     */
    private String decryptKeyName(byte[] data) {
        masterKeyChangeLock.readLock().lock();

        try {
            ByteBuffer buf = ByteBuffer.wrap(data);

            int keyLen = buf.getInt();

            byte[] encKey = new byte[keyLen];

            buf.get(encKey);

            byte[] encKeyName = new byte[buf.remaining()];

            buf.get(encKeyName);

            byte[] serKeyName = getSpi().decrypt(encKeyName, getSpi().decryptKey(encKey));

            return U.fromBytes(serKeyName);
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
    }

    /** */
    private void checkState() {
        if (disconnected) {
            throw new IgniteClientDisconnectedException(ctx.cluster().clientReconnectFuture(),
                "Failed to perform operation, client node disconnected.");
        }

        if (stopped)
            throw new IgniteException("Failed to perform operation, node is stopping.");
    }

    /**
     * Removes encryption key.
     *
     * @param grpId Group id.
     */
    private void removeGroupKey(int grpId) {
        synchronized (metaStorageMux) {
            ctx.cache().context().database().checkpointReadLock();

            try {
                grpEncKeys.remove(grpId);

                metaStorage.remove(ENCRYPTION_KEY_PREFIX + grpId);

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
     * @param grpId Group id.
     * @param encKey Encryption key
     */
    public void beforeCacheGroupStart(int grpId, @Nullable byte[] encKey) {
        if (encKey == null || ctx.clientNode())
            return;

        groupKey(grpId, encKey);
    }

    /**
     * Callback for cache group destroy event.
     * @param grpId Group id.
     */
    public void onCacheGroupDestroyed(int grpId) {
        if (groupKey(grpId) == null)
            return;

        removeGroupKey(grpId);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        try {
            // There is no need to set master key in case of recovery, as it is already relevant.
            if (!forceWriteAllKeysToMetaStore) {
                String masterKeyName = (String)metastorage.read(MASTER_KEY_NAME_PREFIX);

                if (masterKeyName != null)
                    getSpi().setMasterKeyName(masterKeyName);
            }

            metastorage.iterate(ENCRYPTION_KEY_PREFIX, (key, val) -> {
                Integer grpId = Integer.valueOf(key.replace(ENCRYPTION_KEY_PREFIX, ""));

                byte[] encGrpKey = (byte[])val;

                grpEncKeys.computeIfAbsent(grpId, k -> getSpi().decryptKey(encGrpKey));
            }, true);

            if (!grpEncKeys.isEmpty()) {
                U.quietAndInfo(log, "Encryption keys loaded from metastore. [grps=" +
                    F.concat(grpEncKeys.keySet(), ",") + ", masterKeyName=" + getSpi().getMasterKeyName() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read encryption keys state.", e);
        }

        String newMasterKeyName = IgniteSystemProperties.getString(IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP);

        if (newMasterKeyName != null) {
            log.info("System property " + IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP + " is set. Master key " +
                "will be changed locally and group keys will be re-encrypted before join to cluster [masterKeyName=" +
                newMasterKeyName + ']');

            changeMasterKeyAndReencryptGroupKeys(newMasterKeyName);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metaStorage) throws IgniteCheckedException {
        synchronized (metaStorageMux) {
            this.metaStorage = metaStorage;

            writeToMetaStoreEnabled = true;

            writeAllToMetaStore();
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        masterKeyChangeLock.readLock().lock();

        try {
            synchronized (metaStorageMux) {
                writeToMetaStoreEnabled = metaStorage != null;

                if (writeToMetaStoreEnabled)
                    writeAllToMetaStore();
            }
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        masterKeyChangeLock.readLock().lock();

        try {
            synchronized (metaStorageMux) {
                writeToMetaStoreEnabled = false;
            }
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
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

    /** @return {@code True} if the master key change process in progress. */
    public boolean isMasterKeyChangeInProgress() {
        return pendingMasterKey != null;
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
     * Writes all unsaved grpEncKeys to metaStorage.
     * @throws IgniteCheckedException If failed.
     */
    private void writeAllToMetaStore() throws IgniteCheckedException {
        if (!forceWriteAllKeysToMetaStore) {
            for (Map.Entry<Integer, Serializable> entry : grpEncKeys.entrySet()) {
                if (metaStorage.read(ENCRYPTION_KEY_PREFIX + entry.getKey()) != null)
                    continue;

                writeToMetaStore(entry.getKey(), getSpi().encryptKey(entry.getValue()));
            }
        }
        else {
            MasterKeyChangeRecord rec = prepareMasterKeyChangeRecord();

            ctx.cache().context().wal().log(rec);

            metaStorage.write(MASTER_KEY_NAME_PREFIX, rec.getMasterKeyName());

            for (Map.Entry<Integer, byte[]> entry : rec.getGrpKeys().entrySet())
                metaStorage.write(ENCRYPTION_KEY_PREFIX + entry.getKey(), entry.getValue());

            forceWriteAllKeysToMetaStore = false;
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
     * Writes encryption key to metastore.
     *
     * @param grpId Group id.
     * @param encGrpKey Group encryption key.
     */
    private void writeToMetaStore(int grpId, byte[] encGrpKey) {
        if (metaStorage == null || !writeToMetaStoreEnabled)
            return;

        ctx.cache().context().database().checkpointReadLock();

        try {
            metaStorage.write(ENCRYPTION_KEY_PREFIX + grpId, encGrpKey);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to write cache group encryption key [grpId=" + grpId + ']', e);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
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
    @Nullable private HashMap<Integer, byte[]> knownEncryptionKeys() {
        if (F.isEmpty(grpEncKeys))
            return null;

        HashMap<Integer, byte[]> knownKeys = new HashMap<>();

        for (Map.Entry<Integer, Serializable> entry : grpEncKeys.entrySet())
            knownKeys.put(entry.getKey(), getSpi().encryptKey(entry.getValue()));

        return knownKeys;
    }

    /**
     * Generates required count of encryption keys.
     *
     * @param keyCnt Keys count.
     * @return Tuple of collection with newly generated encryption keys and master key digest.
     */
    private T2<Collection<byte[]>, byte[]> createKeys(int keyCnt) {
        masterKeyChangeLock.readLock().lock();

        try {
            if (keyCnt == 0)
                return new T2<>(Collections.emptyList(), getSpi().masterKeyDigest());

            List<byte[]> encKeys = new ArrayList<>(keyCnt);

            for (int i = 0; i < keyCnt; i++)
                encKeys.add(getSpi().encryptKey(getSpi().create()));

            return new T2<>(encKeys, getSpi().masterKeyDigest());
        }
        finally {
            masterKeyChangeLock.readLock().unlock();
        }
    }

    /**
     * Apply keys from WAL record during recovery phase.
     *
     * @param rec Record.
     */
    public void applyKeys(MasterKeyChangeRecord rec) {
        assert !writeToMetaStoreEnabled && !ctx.state().clusterState().active();

        try {
            getSpi().setMasterKeyName(rec.getMasterKeyName());

            for (Map.Entry<Integer, byte[]> entry : rec.getGrpKeys().entrySet())
                grpEncKeys.computeIfAbsent(entry.getKey(), k -> getSpi().decryptKey(entry.getValue()));

            forceWriteAllKeysToMetaStore = true;
        } catch (IgniteSpiException e) {
            log.warning("Unable to apply group keys from WAL record [masterKeyName=" + rec.getMasterKeyName() + ']', e);
        }
    }

    /**
     * Sets up master key and re-encrypt group keys. Writes changes to WAL and if possible to MetaStorage.
     *
     * @param name New master key name.
     */
    private void changeMasterKeyAndReencryptGroupKeys(String name) {
        log.info("Start master key change [masterKeyName=" + name + ']');

        masterKeyChangeLock.writeLock().lock();

        try {
            getSpi().setMasterKeyName(name);

            MasterKeyChangeRecord rec = prepareMasterKeyChangeRecord();

            ctx.cache().context().database().checkpointReadLock();

            try {
                ctx.cache().context().wal().log(rec);

                synchronized (metaStorageMux) {
                    if (writeToMetaStoreEnabled) {
                        metaStorage.write(MASTER_KEY_NAME_PREFIX, rec.getMasterKeyName());

                        for (Map.Entry<Integer, byte[]> entry : rec.getGrpKeys().entrySet())
                            metaStorage.write(ENCRYPTION_KEY_PREFIX + entry.getKey(), entry.getValue());
                    }
                    else
                        forceWriteAllKeysToMetaStore = true;
                }

                log.info("Master key successfully changed [masterKeyName=" + name + ']');
            }
            finally {
                ctx.cache().context().database().checkpointReadUnlock();
            }
        }
        catch (Exception e) {
            U.error(log, "Unable to change master key locally.", e);

            ctx.failure().process(new FailureContext(CRITICAL_ERROR,
                new Exception("Unable to change master key locally.", e)));
        }
        finally {
            masterKeyChangeLock.writeLock().unlock();
        }
    }

    /** @return Master key change record. */
    private MasterKeyChangeRecord prepareMasterKeyChangeRecord() {
        Map<Integer, byte[]> reencryptedKeys = new HashMap<>();

        for (Map.Entry<Integer, Serializable> entry : grpEncKeys.entrySet())
            reencryptedKeys.put(entry.getKey(), getSpi().encryptKey(entry.getValue()));

        return new MasterKeyChangeRecord(getSpi().getMasterKeyName(), reencryptedKeys);
    }

    /** Checks that the master key change process supported by all nodes in cluster. */
    public void checkMasterKeyChangeSupported() {
        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), MASTER_KEY_CHANGE))
            throw new IllegalStateException("Not all nodes in the cluster support the master key change process.");
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     */
    private void finishMasterKeyChange(UUID reqId, Map<UUID, Exception> err) {
        if (pendingMasterKey != null && pendingMasterKey.requestId().equals(reqId))
            pendingMasterKey = null;

        synchronized (opsMux) {
            if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone() &&
                masterKeyChangeFut.id().equals(reqId)) {
                if (err != null && !err.isEmpty()) {
                    Exception e = err.values().stream().findFirst().get();

                    masterKeyChangeFut.onDone(e);
                }
                else
                    masterKeyChangeFut.onDone();
            }
        }
    }

    /**
     * @param msg Error message.
     */
    private void cancelFutures(String msg) {
        for (GenerateEncryptionKeyFuture fut : genEncKeyFuts.values())
            fut.onDone(new IgniteFutureCancelledException(msg));

        if (masterKeyChangeFut != null && !masterKeyChangeFut.isDone())
            masterKeyChangeFut.onDone(new IgniteFutureCancelledException(msg));
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

    /**
     * Pending master key.
     */
    private static class PendingMasterKey implements Serializable {
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
        private PendingMasterKey(UUID reqId, byte[] encKeyName, byte[] digest) {
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

            if (!(o instanceof PendingMasterKey))
                return false;

            PendingMasterKey key = (PendingMasterKey)o;

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
            return S.toString(PendingMasterKey.class, this);
        }
    }

    /** */
    private static class MasterKeyChangeResult implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /** */
    public static class NodeEncryptionKeys implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        NodeEncryptionKeys(Map<Integer, byte[]> knownKeys, Map<Integer, byte[]> newKeys, byte[] masterKeyDigest) {
            this.knownKeys = knownKeys;
            this.newKeys = newKeys;
            this.masterKeyDigest = masterKeyDigest;
        }

        /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node. */
        Map<Integer, byte[]> knownKeys;

        /**  New keys i.e. keys for a local statically configured caches. */
        Map<Integer, byte[]> newKeys;

        /** Master key digest. */
        byte[] masterKeyDigest;
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

    /**
     * Master key change future.
     */
    private static class MasterKeyChangeFuture extends GridFutureAdapter<Void> {
        /** */
        private final UUID id;

        /** @param id Future ID. */
        private MasterKeyChangeFuture(UUID id) {
            this.id = id;
        }

        /** */
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MasterKeyChangeFuture.class, this);
        }
    }
}
