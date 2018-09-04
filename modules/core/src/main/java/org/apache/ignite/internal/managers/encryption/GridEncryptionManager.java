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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.lang.GridPlainClosure;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ENCRYPTION_MGR;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_ENCRYPTION_MASTER_KEY_DIGEST;

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
 * @see GridCacheProcessor#genEncKeysAndStartCacheAfter(Collection, GridPlainClosure)
 */
public class GridEncryptionManager extends GridManagerAdapter<EncryptionSpi> implements MetastorageLifecycleListener,
    IgniteChangeGlobalStateSupport {
    /**
     * Cache encryption introduced in this Ignite version.
     */
    private static final IgniteProductVersion CACHE_ENCRYPTION_SINCE = IgniteProductVersion.fromString("2.7.0");

    /** Synchronization mutex. */
    private final Object mux = new Object();

    /** Flag to enable/disable write to metastore on cluster state change. */
    private volatile boolean writeToMetaStoreEnabled;

    /** Prefix for a encryption group key in meta store. */
    public static final String ENCRYPTION_KEY_PREFIX = "grp-encryption-key-";

    /** Encryption key predicate for meta store. */
    private static final IgnitePredicate<String> ENCRYPTION_KEY_PREFIX_PRED =
        (IgnitePredicate<String>)key -> key.startsWith(ENCRYPTION_KEY_PREFIX);

    /** Group encryption keys. */
    private Map<Integer, Serializable> grpEncKeys = new HashMap<>();

    /** Metastorage. */
    private volatile ReadWriteMetastorage metaStorage;

    /**
     * @param ctx Kernal context.
     */
    public GridEncryptionManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getEncryptionSpi());

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        ctx.addNodeAttribute(ATTR_ENCRYPTION_MASTER_KEY_DIGEST, getSpi().masterKeyDigest());

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        ctx.discovery().localJoinFuture().listen(f -> {
            if (notCoordinator())
                return;

            HashMap<Integer, byte[]> knownEncKeys = knownEncKeys();

            HashMap<Integer, byte[]> newEncKeys =
                newEncKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

            if (newEncKeys == null)
                return;

            for (Map.Entry<Integer, byte[]> entry : newEncKeys.entrySet()) {
                groupKey(entry.getKey(), entry.getValue());

                if (log.isInfoEnabled())
                    log.info("Added encryption key on local join [grpId=" + entry.getKey() + "]");
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node,
        JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = super.validateNode(node, discoData);

        if (res != null)
            return res;

        if (node.isClient())
            return null;

        res = validateNode(node);

        if (res != null)
            return res;

        if (!discoData.hasJoiningNodeData()) {
            if (log.isInfoEnabled())
                log.info("Joining node doesn't have encryption data [node=" + node.id() + "]");

            return null;
        }

        NodeEncryptionKeys nodeEncKeys = (NodeEncryptionKeys)discoData.joiningNodeData();

        if (nodeEncKeys == null || F.isEmpty(nodeEncKeys.knownKeys)) {
            if (log.isInfoEnabled())
                log.info("Joining node doesn't have stored group keys [node=" + node.id() + "]");

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
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = super.validateNode(node);

        if (res != null)
            return res;

        if (node.isClient())
            return null;

        byte[] lclMkDig = getSpi().masterKeyDigest();

        byte[] rmtMkDig = node.attribute(ATTR_ENCRYPTION_MASTER_KEY_DIGEST);

        if (Arrays.equals(lclMkDig, rmtMkDig))
            return null;

        return new IgniteNodeValidationResult(ctx.localNodeId(),
            "Master key digest differs! Node join is rejected. [node=" + node.id() + "]",
            "Master key digest differs! Node join is rejected.");
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        HashMap<Integer, byte[]> knownEncKeys = knownEncKeys();

        HashMap<Integer, byte[]> newKeys =
            newEncKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

        if ((knownEncKeys == null && newKeys == null) || dataBag.isJoiningNodeClient())
            return;

        if (log.isInfoEnabled()) {
            String knownGrps = F.isEmpty(knownEncKeys) ? null : F.concat(knownEncKeys.keySet(), ",");

            if (knownGrps != null)
                log.info("Sending stored group keys to coordinator [grps=" + knownGrps + "]");

            String newGrps = F.isEmpty(newKeys) ? null : F.concat(newKeys.keySet(), ",");

            if (newGrps != null)
                log.info("Sending new group keys to coordinator [grps=" + newGrps + "]");
        }

        dataBag.addJoiningNodeData(ENCRYPTION_MGR.ordinal(), new NodeEncryptionKeys(knownEncKeys, newKeys));
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData data) {
        NodeEncryptionKeys nodeEncryptionKeys = (NodeEncryptionKeys)data.joiningNodeData();

        if (nodeEncryptionKeys == null || nodeEncryptionKeys.newKeys == null || ctx.clientNode())
            return;

        for (Map.Entry<Integer, byte[]> entry : nodeEncryptionKeys.newKeys.entrySet()) {
            if (groupKey(entry.getKey()) == null) {
                if (log.isInfoEnabled()) {
                    log.info("Store group key received from joining node [node=" +
                        data.joiningNodeId() + ", grp=" + entry.getKey() + "]");
                }

                groupKey(entry.getKey(), entry.getValue());
            }
            else if (log.isInfoEnabled()) {
                log.info("Skip group key received from joining node. Already exists. [node=" +
                    data.joiningNodeId() + ", grp=" + entry.getKey() + "]");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (notCoordinator() || dataBag.isJoiningNodeClient())
            return;

        if (dataBag.commonDataCollectedFor(ENCRYPTION_MGR.ordinal()))
            return;

        HashMap<Integer, byte[]> knownEncKeys = knownEncKeys();

        HashMap<Integer, byte[]> newKeys =
            newEncKeys(knownEncKeys == null ? Collections.EMPTY_SET : knownEncKeys.keySet());

        if (knownEncKeys == null)
            knownEncKeys = newKeys;
        else if (newKeys != null){
            for (Map.Entry<Integer, byte[]> entry : newKeys.entrySet()) {
                byte[] old = knownEncKeys.putIfAbsent(entry.getKey(), entry.getValue());

                assert old == null;
            }
        }

        dataBag.addGridCommonData(ENCRYPTION_MGR.ordinal(), knownEncKeys);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<Integer, byte[]> encKeysFromCluster = (Map<Integer, byte[]>)data.commonData();

        if (F.isEmpty(encKeysFromCluster))
            return;

        for (Map.Entry<Integer, byte[]> entry : encKeysFromCluster.entrySet()) {
            if (groupKey(entry.getKey()) == null) {
                if (log.isInfoEnabled())
                    log.info("Store group key received from coordinator [grp=" + entry.getKey() + "]");

                groupKey(entry.getKey(), entry.getValue());
            }
            else if (log.isInfoEnabled())
                log.info("Skip group key received from coordinator. Already exists. [grp=" + entry.getKey() + "]");
        }
    }

    /**
     * Returns group encryption key.
     *
     * @param grpId Group id.
     * @return Group encryption key.
     */
    @Nullable public Serializable groupKey(int grpId) {
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

        Serializable encKey = getSpi().decryptKey(encGrpKey);

        synchronized (mux) {
            if (log.isDebugEnabled())
                log.debug("Key added. [grp=" + grpId + "]");

            grpEncKeys.put(grpId, encKey);

            writeToMetaStore(grpId, encGrpKey);
        }
    }

    /**
     * Removes encryption key.
     *
     * @param grpId Group id.
     */
    private void removeGroupKey(int grpId) {
        synchronized (mux) {
            ctx.cache().context().database().checkpointReadLock();

            try {
                grpEncKeys.remove(grpId);

                metaStorage.remove(ENCRYPTION_KEY_PREFIX + grpId);

                if (log.isDebugEnabled())
                    log.debug("Key removed. [grp=" + grpId + "]");
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to clear meta storage", e);
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
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        try {
            Map<String, ? extends Serializable> encKeys = metastorage.readForPredicate(ENCRYPTION_KEY_PREFIX_PRED);

            if (encKeys.isEmpty())
                return;

            for (String key : encKeys.keySet()) {
                Integer grpId = Integer.valueOf(key.replace(ENCRYPTION_KEY_PREFIX, ""));

                byte[] encGrpKey = (byte[])encKeys.get(key);

                grpEncKeys.putIfAbsent(grpId, getSpi().decryptKey(encGrpKey));
            }


            if (log.isInfoEnabled()) {
                if (!grpEncKeys.isEmpty()) {
                    log.info("Encryption keys loaded from metastore. [grps=" +
                        F.concat(grpEncKeys.keySet(), ",") + "]");
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read encryption keys state.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metaStorage) throws IgniteCheckedException {
        synchronized (mux) {
            this.metaStorage = metaStorage;

            writeToMetaStoreEnabled = true;

            writeAllToMetaStore();
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        synchronized (mux) {
            writeToMetaStoreEnabled = metaStorage != null;

            if (writeToMetaStoreEnabled)
                writeAllToMetaStore();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        synchronized (mux) {
            writeToMetaStoreEnabled = false;
        }
    }

    /**
     * Writes all unsaved grpEncKeys to metaStorage.
     * @throws IgniteCheckedException If failed.
     */
    private void writeAllToMetaStore() throws IgniteCheckedException {
        for (Map.Entry<Integer, Serializable> entry : grpEncKeys.entrySet()) {
            if (metaStorage.read(ENCRYPTION_KEY_PREFIX + entry.getKey()) != null)
                continue;

            writeToMetaStore(entry.getKey(), getSpi().encryptKey(entry.getValue()));
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
    @Nullable private HashMap<Integer, byte[]> newEncKeys(Set<Integer> knownKeys) {
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
    @Nullable private HashMap<Integer, byte[]> knownEncKeys() {
        if (F.isEmpty(grpEncKeys))
            return null;

        HashMap<Integer, byte[]> knownKeys = new HashMap<>();

        for (Map.Entry<Integer, Serializable> entry : grpEncKeys.entrySet())
            knownKeys.put(entry.getKey(), getSpi().encryptKey(entry.getValue()));

        return knownKeys;
    }

    /**
     * Checks whether local node is coordinator. Nodes that are leaving or failed
     * (but are still in topology) are removed from search.
     *
     * @return {@code true} if local node is coordinator.
     */
    private boolean notCoordinator() {
        DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            return !((TcpDiscoverySpi)spi).isLocalNodeCoordinator();
        else {
            ClusterNode crd = null;

            for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
                if (crd == null || crd.order() > node.order())
                    crd = node;
            }

            return crd == null || !F.eq(ctx.localNodeId(), crd.id());
        }
    }

    /** */
    public static class NodeEncryptionKeys implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        NodeEncryptionKeys(Map<Integer, byte[]> knownKeys, Map<Integer, byte[]> newKeys) {
            this.knownKeys = knownKeys;
            this.newKeys = newKeys;
        }

        /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node. */
        Map<Integer, byte[]> knownKeys;

        /**  New keys i.e. keys for a local statically configured caches. */
        Map<Integer, byte[]> newKeys;
    }
}
