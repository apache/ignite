// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheAffinity0<K> {
    /**
     * Gets number of partitions in cache according to configured affinity function.
     *
     * @return Number of cache partitions.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public int partitions();

    /**
     * Gets partition id for the given key.
     *
     * @param key Key to get partition id for.
     * @return Partition id.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public int partition(K key);

    /**
     * Returns {@code true} if given node is the primary node for given key.
     * To check if local node is primary for given key, pass
     * {@link Grid#localNode()} as first parameter.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is the primary node for given key.
     */
    public boolean primary(GridNode n, K key);

    /**
     * Returns {@code true} if local node is one of the backup nodes for given key.
     * To check if local node is primary for given key, pass {@link Grid#localNode()}
     * as first parameter.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is one of the backup nodes for given key.
     */
    public boolean backup(GridNode n, K key);

    /**
     * Returns {@code true} if local node is primary or one of the backup nodes
     * for given key. To check if local node is primary or backup for given key, pass
     * {@link Grid#localNode()} as first parameter.
     * <p>
     * This method is essentially equivalent to calling
     * <i>"{@link #primary(GridNode, Object)} || {@link #backup(GridNode, Object)})"</i>,
     * however it is more efficient as it makes both checks at once.
     *
     * @param n Node to check.
     * @param key Key to check.
     * @return {@code True} if local node is primary or backup for given key.
     */
    public boolean primaryOrBackup(GridNode n, K key);

    /**
     * Gets partition ids for which nodes of the given projection has primary
     * ownership.
     * <p>
     * Note that since {@link GridNode} implements {@link GridProjection},
     * to find out primary partitions for a single node just pass
     * a single node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has primary ownership.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public int[] primaryPartitions(GridNode n);

    /**
     * Gets partition ids for which nodes of the given projection has backup
     * ownership. Note that you can find a back up at a certain level, e.g.
     * {@code first} backup or {@code third} backup by specifying the
     * {@code 'levels} parameter. If no {@code 'level'} is specified then
     * all backup partitions are returned.
     * <p>
     * Note that since {@link GridNode} implements {@link GridProjection},
     * to find out backup partitions for a single node, just pass that single
     * node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has backup ownership.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public int[] backupPartitions(GridNode n);

    /**
     * Gets partition ids for which nodes of the given projection has ownership
     * (either primary or backup).
     * <p>
     * Note that since {@link GridNode} implements {@link GridProjection},
     * to find out all partitions for a single node, just pass that single
     * node into this method.
     * <p>
     * This method may return an empty array if none of nodes in the projection
     * have nearOnly disabled.
     *
     * @param n Grid node.
     * @return Partition ids for which given projection has ownership.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public int[] allPartitions(GridNode n);

    /**
     * Maps passed in key to a key which will be used for node affinity. The affinity
     * key may be different from actual key if some field in the actual key was
     * designated for affinity mapping via {@link GridCacheAffinityMapped} annotation
     * or if a custom {@link GridCacheAffinityMapper} was configured.
     *
     * @param key Key to map.
     * @return Key to be used for node-to-affinity mapping (may be the same
     *      key as passed in).
     */
    public Object affinityKey(K key);

    /**
     * This method provides ability to detect which keys are mapped to which nodes.
     * Use it to determine which nodes are storing which keys prior to sending
     * jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node mapped to all keys.</li>
     * <li>
     *      For fully replicated caches {@link GridCacheAffinity} is
     *      used to determine which keys are mapped to which nodes.
     * </li>
     * <li>For partitioned caches, the returned map represents node-to-key affinity.</li>
     * </ul>
     *
     * @param keys Keys to map to nodes.
     * @return Map of nodes to keys or empty map if there are no alive nodes for this cache.
     */
    public Map<GridNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys);

    /**
     * This method provides ability to detect to which primary node the given key
     * is mapped. Use it to determine which nodes are storing which keys prior to sending
     * jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node ID.</li>
     * <li>
     *      For fully replicated caches first node ID returned by {@link GridCacheAffinity}
     *      is returned.
     * </li>
     * <li>For partitioned caches, primary node for the given key is returned.</li>
     * </ul>
     *
     * @param key Keys to map to a node.
     * @return Primary node for the key or {@code null} if there are no alive nodes for this cache.
     */
    @Nullable public GridNode mapKeyToNode(K key);

    /**
     * Gets primary and backup nodes for the key. Note that primary node is always
     * first in the returned collection.
     * <p>
     * If there are only cache nodes in the projection with
     * {@link GridCacheConfiguration#getPartitionedDistributionMode()} property set to {@code NEAR_ONLY}, then this
     * method will return an empty collection.
     *
     * @param key Key to get affinity nodes for.
     * @return Collection of primary and backup nodes for the key with primary node
     *      always first, or an empty collection if this projection contains only nodes with
     *      {@link GridCacheConfiguration#getPartitionedDistributionMode()} property set to {@code NEAR_ONLY}.
     */
    public Collection<GridNode> mapKeyToPrimaryAndBackups(K key);

    /**
     * Gets primary node for the given partition.
     *
     * @param part Partition id.
     * @return Primary node for the given partition.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public GridNode mapPartitionToNode(int part);

    /**
     * Gets primary nodes for the given partitions.
     *
     * @param parts Partition ids.
     * @return Mapping of given partitions to their primary nodes.
     * @see GridCacheAffinity
     * @see GridCacheConfiguration#getAffinity()
     * @see GridCacheConfiguration#setAffinity(GridCacheAffinity)
     */
    public Map<Integer, GridNode> mapPartitionsToNodes(Collection<Integer> parts);

    /**
     * Gets primary and backup nodes for partition. Note that primary node is always
     * first in the returned collection.
     * <p>
     * If there are only cache nodes in the projection with
     * {@link GridCacheConfiguration#getPartitionedDistributionMode()} property set to {@code NEAR_ONLY}, then this
     * method will return an empty collection.
     *
     * @param part Partition to get affinity nodes for.
     * @return Collection of primary and backup nodes for partition with primary node
     *      always first, or an empty collection if this projection contains only nodes with
     *      {@link GridCacheConfiguration#getPartitionedDistributionMode()} property set to {@code NEAR_ONLY}.
     */
    public Collection<GridNode> mapPartitionToPrimaryAndBackups(int part);
}
