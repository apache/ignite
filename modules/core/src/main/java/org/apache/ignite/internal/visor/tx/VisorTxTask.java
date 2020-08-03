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

package org.apache.ignite.internal.visor.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteTxMappings;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxRemoteEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 *
 */
@GridInternal
public class VisorTxTask extends VisorMultiNodeTask<VisorTxTaskArg, Map<ClusterNode, VisorTxTaskResult>, VisorTxTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorTxTaskArg, VisorTxTaskResult> job(VisorTxTaskArg arg) {
        return new VisorTxJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorTxTaskArg> arg) {
        final VisorTxTaskArg taskArg = arg.getArgument();

        if (taskArg.getConsistentIds() != null) {
            return F.transform(ignite.cluster().forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return taskArg.getConsistentIds().contains((String)node.consistentId().toString());
                }
            }).nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        if (taskArg.getProjection() == VisorTxProjection.SERVER) {
            return F.transform(ignite.cluster().forServers().nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        if (taskArg.getProjection() == VisorTxProjection.CLIENT) {
            return F.transform(ignite.cluster().forClients().nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        return F.transform(ignite.cluster().nodes(), new IgniteClosure<ClusterNode, UUID>() {
            @Override public UUID apply(ClusterNode node) {
                return node.id();
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<ClusterNode, VisorTxTaskResult> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, VisorTxTaskResult> mapRes = new TreeMap<>();

        Map<UUID, ClusterNode> nodeMap = new HashMap<>();

        for (ComputeJobResult result : results) {
            VisorTxTaskResult data = result.getData();

            if (data == null || data.getInfos().isEmpty())
                continue;

            mapRes.put(result.getNode(), data);

            nodeMap.put(result.getNode().id(), result.getNode());
        }

        if (!taskArg.verboseMode()) {
            // If not in verbose mode, remove local and remote txs for which near txs are present.
            for (VisorTxTaskResult result : mapRes.values()) {
                List<VisorTxInfo> infos = result.getInfos();

                Iterator<VisorTxInfo> it = infos.iterator();

                while (it.hasNext()) {
                    VisorTxInfo info = it.next();

                    if (!info.getXid().equals(info.getNearXid())) {
                        UUID nearNodeId = info.getMasterNodeIds().iterator().next();

                        // Try find id.
                        ClusterNode node = nodeMap.get(nearNodeId);

                        if (node == null)
                            continue;

                        VisorTxTaskResult res0 = mapRes.get(node);

                        if (res0 == null)
                            continue;

                        boolean exists = false;

                        for (VisorTxInfo txInfo : res0.getInfos()) {
                            if (txInfo.getXid().equals(info.getNearXid())) {
                                exists = true;

                                break;
                            }
                        }

                        if (exists)
                            it.remove();
                    }
                }
            }
        }

        return mapRes;
    }

    /**
     *
     */
    private static class VisorTxJob extends VisorJob<VisorTxTaskArg, VisorTxTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final int DEFAULT_LIMIT = 50;

        /** */
        private static final TxKillClosure NEAR_KILL_CLOSURE = new NearKillClosure();

        /** */
        private static final TxKillClosure LOCAL_KILL_CLOSURE = new LocalKillClosure();

        /** */
        private static final TxKillClosure REMOTE_KILL_CLOSURE = new RemoteKillClosure();

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorTxJob(VisorTxTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorTxTaskResult run(@Nullable VisorTxTaskArg arg) throws IgniteException {
            if (arg == null)
                return new VisorTxTaskResult(Collections.emptyList());

            IgniteTxManager tm = ignite.context().cache().context().tm();

            Collection<IgniteInternalTx> transactions = tm.activeTransactions();

            List<VisorTxInfo> infos = new ArrayList<>();

            int limit = arg.getLimit() == null ? DEFAULT_LIMIT : arg.getLimit();

            Pattern lbMatch = null;

            if (arg.getLabelRegex() != null) {
                try {
                    lbMatch = Pattern.compile(arg.getLabelRegex());
                }
                catch (PatternSyntaxException ignored) {
                    // No-op.
                }
            }

            for (IgniteInternalTx locTx : transactions) {
                if (arg.verboseMode() && !arg.txInfoArgument().gridCacheVersion().equals(locTx.nearXidVersion()))
                    continue;

                if (arg.getXid() != null && !locTx.xid().toString().equals(arg.getXid()))
                    continue;

                if (arg.getState() != null && locTx.state() != arg.getState())
                    continue;

                long duration = U.currentTimeMillis() - locTx.startTime();

                if (arg.getMinDuration() != null && duration < arg.getMinDuration())
                    continue;

                String lb = null;
                int size = 0;
                Collection<UUID> mappings = null;
                TxKillClosure killClo = null;

                // This filter conditions have meaning only for near txs, so we skip dht because it will never match.
                boolean skip = arg.getMinSize() != null || lbMatch != null;

                if (locTx instanceof GridNearTxLocal) {
                    GridNearTxLocal locTx0 = (GridNearTxLocal)locTx;

                    lb = locTx0.label();

                    if (lbMatch != null && !lbMatch.matcher(lb == null ? "null" : lb).matches())
                        continue;

                    mappings = new ArrayList<>();

                    if (locTx0.mappings() != null) {
                        IgniteTxMappings txMappings = locTx0.mappings();

                        for (GridDistributedTxMapping mapping :
                            txMappings.single() ? Collections.singleton(txMappings.singleMapping()) : txMappings.mappings()) {
                            if (mapping == null)
                                continue;

                            mappings.add(mapping.primary().id());

                            size += mapping.entries().size(); // Entries are not synchronized so no visibility guaranties for size.
                        }
                    }

                    if (arg.getMinSize() != null && size < arg.getMinSize())
                        continue;

                    killClo = NEAR_KILL_CLOSURE;
                }
                else if (locTx instanceof GridDhtTxLocal) {
                    if (skip)
                        continue;

                    GridDhtTxLocal locTx0 = (GridDhtTxLocal)locTx;

                    Map<UUID, GridDistributedTxMapping> dhtMap = U.field(locTx0, "dhtMap");

                    mappings = new ArrayList<>();

                    if (dhtMap != null) {
                        for (GridDistributedTxMapping mapping : dhtMap.values()) {
                            mappings.add(mapping.primary().id());

                            size += mapping.entries().size();
                        }
                    }

                    Map<UUID, GridDistributedTxMapping> nearMap = U.field(locTx, "nearMap");

                    if (nearMap != null) {
                        for (GridDistributedTxMapping mapping : nearMap.values()) {
                            mappings.add(mapping.primary().id());

                            size += mapping.entries().size();
                        }
                    }

                    killClo = LOCAL_KILL_CLOSURE;
                }
                else if (locTx instanceof GridDhtTxRemote) {
                    if (skip)
                        continue;

                    GridDhtTxRemote locTx0 = (GridDhtTxRemote)locTx;

                    size = locTx0.readMap().size() + locTx.writeMap().size();

                    killClo = REMOTE_KILL_CLOSURE;
                }

                TxVerboseInfo verboseInfo = arg.verboseMode() ? createVerboseInfo(ignite, locTx) : null;

                infos.add(new VisorTxInfo(locTx.xid(), locTx.startTime(), duration, locTx.isolation(),
                    locTx.concurrency(), locTx.timeout(), lb, mappings, locTx.state(), size,
                    locTx.nearXidVersion().asIgniteUuid(), locTx.masterNodeIds(), locTx.topologyVersionSnapshot(),
                    verboseInfo));

                if (arg.getOperation() == VisorTxOperation.KILL)
                    killClo.apply(locTx, tm);

                if (infos.size() == limit)
                    break;
            }

            // If transaction was not found in verbose --tx --info mode, try to fetch it from history.
            if (arg.verboseMode() && infos.isEmpty()) {
                Object completed = tm.peekCompletedVersionsHistory(arg.txInfoArgument().gridCacheVersion());

                if (completed != null) {
                    if (Boolean.TRUE.equals(completed))
                        infos.add(new VisorTxInfo(arg.txInfoArgument().gridCacheVersion().asIgniteUuid(), COMMITTED));
                    else if (Boolean.FALSE.equals(completed))
                        infos.add(new VisorTxInfo(arg.txInfoArgument().gridCacheVersion().asIgniteUuid(), ROLLED_BACK));
                }
            }

            Comparator<VisorTxInfo> comp = TxDurationComparator.INSTANCE;

            if (arg.getSortOrder() != null) {
                switch (arg.getSortOrder()) {
                    case DURATION:
                        comp = TxDurationComparator.INSTANCE;

                        break;

                    case SIZE:
                        comp = TxSizeComparator.INSTANCE;

                        break;

                    case START_TIME:
                        comp = TxStartTimeComparator.INSTANCE;

                        break;

                    default:
                }
            }

            Collections.sort(infos, comp);

            return new VisorTxTaskResult(infos);
        }
    }

    /**
     * Constructs detailed transaction info for verbose mode.
     *
     * @param ignite Ignite.
     * @param locTx Local tx.
     */
    private static TxVerboseInfo createVerboseInfo(IgniteEx ignite, IgniteInternalTx locTx) {
        TxVerboseInfo res = new TxVerboseInfo();

        res.nearXidVersion(locTx.nearXidVersion());

        Map<Integer, String> usedCaches = new HashMap<>();
        Map<Integer, String> usedCacheGroups = new HashMap<>();

        ClusterNode locNode = ignite.context().discovery().localNode();

        res.localNodeId(locNode.id());
        res.localNodeConsistentId(locNode.consistentId());

        if (locTx instanceof GridNearTxLocal) {
            IgniteTxMappings mappings = ((GridNearTxLocal)locTx).mappings();

            List<IgniteTxEntry> nearOnlyEntries = new ArrayList<>();
            List<IgniteTxEntry> locEntries = new ArrayList<>();

            for (GridDistributedTxMapping mapping : mappings.mappings()) {
                if (F.eqNodes(mapping.primary(), locNode))
                    locEntries.addAll(mapping.entries());
                else
                    nearOnlyEntries.addAll(mapping.entries());
            }

            res.nearNodeId(locNode.id());
            res.nearNodeConsistentId(locNode.consistentId());

            res.txMappingType(TxMappingType.NEAR);

            List<TxVerboseKey> nearOnlyTxKeys = fetchTxEntriesAndFillUsedCaches(
                ignite, locTx, usedCaches, usedCacheGroups, nearOnlyEntries, true);

            List<TxVerboseKey> locTxKeys = fetchTxEntriesAndFillUsedCaches(
                ignite, locTx, usedCaches, usedCacheGroups, locEntries, false);

            res.nearOnlyTxKeys(nearOnlyTxKeys);
            res.localTxKeys(locTxKeys);
        }
        else if (locTx instanceof GridDhtTxLocal) {
            UUID nearNodeId = locTx.masterNodeIds().iterator().next();

            DiscoCache discoCache = ignite.context().discovery().discoCache(locTx.topologyVersion());

            if (discoCache == null)
                discoCache = ignite.context().discovery().discoCache();

            ClusterNode nearNode = discoCache.node(nearNodeId);

            res.nearNodeId(nearNodeId);
            res.nearNodeConsistentId(nearNode.consistentId());

            res.txMappingType(TxMappingType.DHT);

            res.localTxKeys(fetchTxEntriesAndFillUsedCaches(
                ignite, locTx, usedCaches, usedCacheGroups, locTx.allEntries(), false));
        }
        else if (locTx instanceof GridDhtTxRemote) {
            Iterator<UUID> masterNodesIter = locTx.masterNodeIds().iterator();

            UUID nearNodeId = masterNodesIter.next();
            UUID dhtNodeId = masterNodesIter.next();

            DiscoCache discoCache = ignite.context().discovery().discoCache(locTx.topologyVersion());

            if (discoCache == null)
                discoCache = ignite.context().discovery().discoCache();

            ClusterNode nearNode = discoCache.node(nearNodeId);
            ClusterNode dhtNode = discoCache.node(dhtNodeId);

            res.nearNodeId(nearNodeId);
            res.nearNodeConsistentId(nearNode.consistentId());

            res.txMappingType(TxMappingType.REMOTE);

            res.dhtNodeId(dhtNodeId);
            res.dhtNodeConsistentId(dhtNode.consistentId());

            res.localTxKeys(fetchTxEntriesAndFillUsedCaches(
                ignite, locTx, usedCaches, usedCacheGroups, locTx.allEntries(), false));
        }

        res.usedCaches(usedCaches);
        res.usedCacheGroups(usedCacheGroups);

        return res;
    }

    /**
     * Retrieves detailed information about used keys and locks ownership.
     *
     * @param ignite Ignite.
     * @param locTx Local tx.
     * @param usedCaches Used caches.
     * @param usedCacheGroups Used cache groups.
     * @param locEntries Local entries.
     * @param skipLocksCheck Skip locks check.
     */
    private static List<TxVerboseKey> fetchTxEntriesAndFillUsedCaches(
        IgniteEx ignite,
        IgniteInternalTx locTx,
        Map<Integer, String> usedCaches,
        Map<Integer, String> usedCacheGroups,
        Collection<IgniteTxEntry> locEntries,
        boolean skipLocksCheck
    ) {
        List<TxVerboseKey> locTxKeys = new ArrayList<>();

        for (IgniteTxEntry txEntry : locEntries) {
            GridCacheContext cacheCtx = ignite.context().cache().context().cacheContext(txEntry.cacheId());

            usedCaches.put(cacheCtx.cacheId(), cacheCtx.name());
            usedCacheGroups.put(cacheCtx.groupId(), cacheCtx.group().cacheOrGroupName());

            TxKeyLockType keyLockType = TxKeyLockType.NO_LOCK;
            GridCacheVersion ownerVer = null;

            if (!skipLocksCheck) {
                GridCacheEntryEx entryEx = cacheCtx.cache().entryEx(txEntry.key(), locTx.topologyVersion());

                Collection<GridCacheMvccCandidate> locCandidates;
                try {
                    locCandidates = entryEx.localCandidates();
                }
                catch (GridCacheEntryRemovedException ignored) {
                    U.warn(ignite.log(), "Failed to process TX key: entry was already removed: " + txEntry.txKey());

                    continue;
                }

                boolean owner = false;
                boolean present = false;

                for (GridCacheMvccCandidate mvccCandidate : locCandidates) {
                    if (mvccCandidate.owner())
                        ownerVer = mvccCandidate.version();

                    if (locTx.xidVersion().equals(mvccCandidate.version())) {
                        present = true;

                        if (mvccCandidate.owner())
                            owner = true;
                    }
                }

                keyLockType = present ?
                    (owner ? TxKeyLockType.OWNS_LOCK : TxKeyLockType.AWAITS_LOCK) :
                    TxKeyLockType.NO_LOCK;
            }

            TxVerboseKey txVerboseKey = new TxVerboseKey(txEntry.txKey().toString(), keyLockType, ownerVer, txEntry.isRead());

            locTxKeys.add(txVerboseKey);
        }

        return locTxKeys;
    }

    /**
     *
     */
    private static class TxStartTimeComparator implements Comparator<VisorTxInfo> {
        /** Instance. */
        public static final TxStartTimeComparator INSTANCE = new TxStartTimeComparator();

        /** {@inheritDoc} */
        @Override public int compare(VisorTxInfo o1, VisorTxInfo o2) {
            return Long.compare(o2.getStartTime(), o1.getStartTime());
        }
    }

    /**
     *
     */
    private static class TxDurationComparator implements Comparator<VisorTxInfo> {
        /** Instance. */
        public static final TxDurationComparator INSTANCE = new TxDurationComparator();

        /** {@inheritDoc} */
        @Override public int compare(VisorTxInfo o1, VisorTxInfo o2) {
            return Long.compare(o2.getDuration(), o1.getDuration());
        }
    }

    /**
     *
     */
    private static class TxSizeComparator implements Comparator<VisorTxInfo> {
        /** Instance. */
        public static final TxSizeComparator INSTANCE = new TxSizeComparator();

        /** {@inheritDoc} */
        @Override public int compare(VisorTxInfo o1, VisorTxInfo o2) {
            return Long.compare(o2.getSize(), o1.getSize());
        }
    }

    /** Type shortcut. */
    private interface TxKillClosure extends
        IgniteBiClosure<IgniteInternalTx, IgniteTxManager, IgniteInternalFuture<IgniteInternalTx>> {
    }

    /** Kills near tx. */
    private static class NearKillClosure implements TxKillClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> apply(IgniteInternalTx tx, IgniteTxManager tm) {
            return tx.isRollbackOnly() || tx.state() == COMMITTING || tx.state() == COMMITTED ?
                new GridFinishedFuture<>() : ((GridNearTxLocal)tx).rollbackNearTxLocalAsync(false, false);
        }
    }

    /** Kills local tx. */
    private static class LocalKillClosure implements TxKillClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> apply(IgniteInternalTx tx, IgniteTxManager tm) {
            return tx.isRollbackOnly() || tx.state() == COMMITTING || tx.state() == COMMITTED ?
                new GridFinishedFuture<>() : ((GridDhtTxLocal)tx).rollbackDhtLocalAsync();
        }
    }

    /** Kills remote tx. */
    private static class RemoteKillClosure implements TxKillClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> apply(IgniteInternalTx tx, IgniteTxManager tm) {
            IgniteTxRemoteEx remote = (IgniteTxRemoteEx)tx;

            if (tx.isRollbackOnly() || tx.state() == COMMITTING || tx.state() == COMMITTED)
                return new GridFinishedFuture<>();

            if (tx.state() == TransactionState.PREPARED)
                remote.doneRemote(tx.xidVersion(),
                    Collections.<GridCacheVersion>emptyList(),
                    Collections.<GridCacheVersion>emptyList(),
                    Collections.<GridCacheVersion>emptyList());

            return tx.rollbackAsync();
        }
    }
}
