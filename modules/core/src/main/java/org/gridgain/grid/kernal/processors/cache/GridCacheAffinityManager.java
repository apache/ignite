/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache affinity manager.
 */
public class GridCacheAffinityManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Factor for maximum number of attempts to calculate all partition affinity keys. */
    private static final int MAX_PARTITION_KEY_ATTEMPT_RATIO = 10;

    /** Affinity cached function. */
    private GridAffinityAssignmentCache aff;

    /** Affinity keys. */
    private GridPartitionLockKey[] partAffKeys;

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        aff = new GridAffinityAssignmentCache(cctx, cctx.namex(), cctx.config().getAffinity(),
            cctx.config().getAffinityMapper(), cctx.config().getBackups());

        // Generate internal keys for partitions.
        int partCnt = partitions();

        partAffKeys = new GridPartitionLockKey[partCnt];

        Collection<Integer> found = new HashSet<>();

        long affKey = 0;

        while (true) {
            GridPartitionLockKey key = new GridPartitionLockKey(affKey);

            int part = aff.partition(key);

            if (found.add(part)) {
                // This is a key for not yet calculated partition.
                key.partitionId(part);

                partAffKeys[part] = key;

                if (found.size() == partCnt)
                    break;
            }

            affKey++;

            if (affKey > partCnt * MAX_PARTITION_KEY_ATTEMPT_RATIO)
                throw new IllegalStateException("Failed to calculate partition affinity keys for given affinity " +
                    "function [attemptCnt=" + affKey + ", found=" + found + ", cacheName=" + cctx.name() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        if (cctx.isLocal())
            // No discovery event needed for local affinity.
            aff.calculate(1, null);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        aff = null;
    }

    /**
     * Gets affinity ready future, a future that will be completed after affinity with given
     * topology version is calculated.
     *
     * @param topVer Topology version to wait.
     * @return Affinity ready future.
     */
    public GridFuture<Long> affinityReadyFuture(long topVer) {
        assert !cctx.isLocal();

        GridFuture<Long> fut = aff.readyFuture(topVer);

        return fut != null ? fut : new GridFinishedFutureEx<>(topVer);
    }

    /**
     * Gets affinity ready future that will be completed after affinity with given topology version is calculated.
     * Will return {@code null} if topology with given version is ready by the moment method is invoked.
     *
     * @param topVer Topology version to wait.
     * @return Affinity ready future or {@code null}.
     */
    @Nullable public GridFuture<Long> affinityReadyFuturex(long topVer) {
        assert !cctx.isLocal();

        return aff.readyFuture(topVer);
    }

    /**
     * Clean up outdated cache items.
     *
     * @param topVer Actual topology version, older versions will be removed.
     */
    public void cleanUpCache(long topVer) {
        assert !cctx.isLocal();

        aff.cleanUpCache(topVer);
    }

    /**
     * Initializes affinity for joined node.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for this topology version.
     */
    public void initializeAffinity(long topVer, List<List<ClusterNode>> affAssignment) {
        assert !cctx.isLocal();

        aff.initialize(topVer, affAssignment);
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignments.
     */
    public List<List<ClusterNode>> assignments(long topVer) {
        if (cctx.isLocal())
            topVer = 1;

        return aff.assignments(topVer);
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity for.
     * @param discoEvt Discovery event that causes this topology change.
     */
    public List<List<ClusterNode>> calculateAffinity(long topVer, GridDiscoveryEvent discoEvt) {
        assert !cctx.isLocal();

        return aff.calculate(topVer, discoEvt);
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        return aff.partitions();
    }

    /**
     * Gets partition affinity key for given partition id. Partition affinity keys are precalculated
     * on manager start.
     *
     * @param partId Partition ID.
     * @return Affinity key.
     */
    public GridPartitionLockKey partitionAffinityKey(int partId) {
        assert partId >=0 && partId < partAffKeys.length;

        return partAffKeys[partId];
    }

    /**
     * NOTE: Use this method always when you need to calculate partition id for
     * a key provided by user. It's required since we should apply affinity mapper
     * logic in order to find a key that will eventually be passed to affinity function.
     *
     * @param key Key.
     * @return Partition.
     */
    public <T> int partition(T key) {
        return aff.partition(key);
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(K key, long topVer) {
        return nodes(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, long topVer) {
        if (cctx.isLocal())
            topVer = 1;

        return aff.nodes(part, topVer);
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primary(K key, long topVer) {
        return primary(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primary(int part, long topVer) {
        List<ClusterNode> nodes = nodes(part, topVer);

        if (nodes.isEmpty())
            return null;

        return nodes.get(0);
    }

    /**
     * @param n Node to check.
     * @param key Key to check.
     * @param topVer Topology version.
     * @return {@code True} if checked node is primary for given key.
     */
    public boolean primary(ClusterNode n, K key, long topVer) {
        return F.eq(primary(key, topVer), n);
    }

    /**
     * @param n Node to check.
     * @param part Partition.
     * @param topVer Topology version.
     * @return {@code True} if checked node is primary for given key.
     */
    public boolean primary(ClusterNode n, int part, long topVer) {
        return F.eq(primary(part, topVer), n);
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return Backup nodes.
     */
    public Collection<ClusterNode> backups(K key, long topVer) {
        return backups(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Backup nodes.
     */
    public Collection<ClusterNode> backups(int part, long topVer) {
        List<ClusterNode> nodes = nodes(part, topVer);

        assert !F.isEmpty(nodes);

        if (nodes.size() <= 1)
            return Collections.emptyList();

        return F.view(nodes, F.notEqualTo(nodes.get(0)));
    }

    /**
     * @param keys keys.
     * @param topVer Topology version.
     * @return Nodes for the keys.
     */
    public Collection<ClusterNode> remoteNodes(Iterable<? extends K> keys, long topVer) {
        Collection<Collection<ClusterNode>> colcol = new GridLeanSet<>();

        for (K key : keys)
            colcol.add(nodes(key, topVer));

        return F.view(F.flatCollections(colcol), F.remoteNodes(cctx.localNodeId()));
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return {@code true} if given key belongs to local node.
     */
    public boolean localNode(K key, long topVer) {
        return localNode(partition(key), topVer);
    }

    /**
     * @param part Partition number to check.
     * @param topVer Topology version.
     * @return {@code true} if given partition belongs to local node.
     */
    public boolean localNode(int part, long topVer) {
        assert part >= 0 : "Invalid partition: " + part;

        return nodes(part, topVer).contains(cctx.localNode());
    }

    /**
     * @param node Node.
     * @param part Partition number to check.
     * @param topVer Topology version.
     * @return {@code true} if given partition belongs to specified node.
     */
    public boolean belongs(ClusterNode node, int part, long topVer) {
        assert node != null;
        assert part >= 0 : "Invalid partition: " + part;

        return nodes(part, topVer).contains(node);
    }

    /**
     * @param node Node.
     * @param key Key to check.
     * @param topVer Topology version.
     * @return {@code true} if given key belongs to specified node.
     */
    public boolean belongs(ClusterNode node, K key, long topVer) {
        assert node != null;

        return belongs(node, partition(key), topVer);
    }

    /**
     * @param nodeId Node ID.
     * @param topVer Topology version to calculate affinity.
     * @return Partitions for which given node is primary.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, long topVer) {
        if (cctx.isLocal())
            topVer = 1;

        return aff.primaryPartitions(nodeId, topVer);
    }

    /**
     * @param nodeId Node ID.
     * @param topVer Topology version to calculate affinity.
     * @return Partitions for which given node is backup.
     */
    public Set<Integer> backupPartitions(UUID nodeId, long topVer) {
        if (cctx.isLocal())
            topVer = 1;

        return aff.backupPartitions(nodeId, topVer);
    }

    /**
     * @return Affinity-ready topology version.
     */
    public long affinityTopologyVersion() {
        return aff.lastVersion();
    }
}
