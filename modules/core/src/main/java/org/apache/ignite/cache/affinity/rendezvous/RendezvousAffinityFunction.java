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

package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityNodeHashResolver;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Affinity function for partitioned cache based on Highest Random Weight algorithm. This function supports the
 * following configuration: <ul> <li> {@code partitions} - Number of partitions to spread across nodes. </li> <li>
 * {@code excludeNeighbors} - If set to {@code true}, will exclude same-host-neighbors from being backups of each other.
 * This flag can be ignored in cases when topology has no enough nodes for assign backups. Note that {@code
 * backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}. </li> <li> {@code backupFilter} -
 * Optional filter for back up nodes. If provided, then only nodes that pass this filter will be selected as backup
 * nodes. If not provided, then primary and backup nodes will be selected out of all nodes available for this cache.
 * </li> </ul> <p> Cache affinity can be configured for individual caches via {@link CacheConfiguration#getAffinity()}
 * method.
 */
public class RendezvousAffinityFunction implements AffinityFunction, Externalizable {
    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 1024;
    /** Default number of buckets. */
    public static final int DFLT_BUCKETS_COUNT = 32;
    /** */
    private static final long serialVersionUID = 0L;
    /** Comparator of hashed nodes. */
    private static final Comparator<IgniteBiTuple<Long, ClusterNode>> COMPARATOR = new HashComparator();
    /** Comparator of hashed bucket indexes. */
    private static final Comparator<IgniteBiTuple<Long, Integer>> BUCKET_COMPARATOR = new HashBucketComparator();
    /**
     * When the version of the oldest node in the topology is greater then compatibilityVersion the
     * RendezvousAffinityFunction switches to the new implementation of the algorithm.
     */
    private final IgniteProductVersion compatibilityVer
        = new IgniteProductVersion((byte)1, (byte)6, (byte)0, 0L, null);
    /**
    * Pre-calculated bucket indexes that are hashed with partition numbers. Used with large nodes number. Nodes array
    * is split to buckets to improve the performance of assignment calculation.
    */
    private transient List<List<Integer>> bucketIdxsByPart;
    /** Thread local message digest. */
    private ThreadLocal<MessageDigest> digest = new ThreadLocal<MessageDigest>() {
        @Override protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException e) {
                assert false : "Should have failed in constructor";

                throw new IgniteException("Failed to obtain message digest (digest was available in constructor)", e);
            }
        }
    };

    /** Number of partitions. */
    private int parts;

    /** Exclude neighbors flag. */
    private boolean exclNeighbors;

    /** Exclude neighbors warning. */
    private transient boolean exclNeighborsWarn;

    /** Optional backup filter. First node is primary, second node is a node being tested. */
    private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

    /** Optional affinity backups filter. The first node is a node being tested,
     *  the second is a list of nodes that are already assigned for a given partition (the first node in the list
     *  is primary). */
    private IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter;

    /** Hash ID resolver. */
    private AffinityNodeHashResolver hashIdRslvr = null;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger instance. */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Empty constructor with all defaults.
     */
    public RendezvousAffinityFunction() {
        this(false);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other and specified
     * number of backups. <p> Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code
     * true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public RendezvousAffinityFunction(boolean exclNeighbors) {
        this(exclNeighbors, DFLT_PARTITION_COUNT);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other, and specified
     * number of backups and partitions. <p> Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is
     * set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     * @param parts Total number of partitions.
     */
    public RendezvousAffinityFunction(boolean exclNeighbors, int parts) {
        this(exclNeighbors, parts, null);
    }

    /**
     * Initializes optional counts for replicas and backups. <p> Note that {@code backupFilter} is ignored if {@code
     * excludeNeighbors} is set to {@code true}.
     *
     * @param parts Total number of partitions.
     * @param backupFilter Optional back up filter for nodes. If provided, backups will be selected from all nodes that
     * pass this filter. First argument for this filter is primary node, and second argument is node being tested. <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     */
    public RendezvousAffinityFunction(int parts, @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this(false, parts, backupFilter);
    }

    /**
     * Private constructor.
     *
     * @param exclNeighbors Exclude neighbors flag.
     * @param parts Partitions count.
     * @param backupFilter Backup filter.
     */
    private RendezvousAffinityFunction(boolean exclNeighbors, int parts,
        IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        A.ensure(parts > 0, "parts > 0");

        this.exclNeighbors = exclNeighbors;
        this.parts = parts;
        this.backupFilter = backupFilter;
        bucketIdxsByPart = calculateBucketIdxs(parts, DFLT_BUCKETS_COUNT);

        try {
            MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteException("Failed to obtain MD5 message digest instance.", e);
        }
    }

    /**
     * The pack partition number and nodeHash.hashCode to long and mix it by hash function based on the Wang/Jenkins
     * hash.
     *
     * @see <a href="https://gist.github.com/badboy/6267743#64-bit-mix-functions">64 bit mix functions</a>
     */
    private static long hash(int key0, int key1) {
        long key = (key0 & 0xFFFFFFFFL)
            | ((key1 & 0xFFFFFFFFL) << 32);
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >>> 24);
        key += (key << 3) + (key << 8); // key * 265
        key ^= (key >>> 14);
        key += (key << 2) + (key << 4); // key * 21
        key ^= (key >>> 28);
        key += (key << 31);
        return key;
    }

    /**
     * Pre-calculate bucket indexes that are hashed with partition numbers. Used with large nodes number. Nodes array
     * is split to buckets to improve the performance of assignment calculation.
     */
    private static List<List<Integer>> calculateBucketIdxs(int parts, int buckets) {
        List<List<Integer>> res = new ArrayList<>(parts);
        for (int part = 0; part < parts; ++part) {
            List<IgniteBiTuple<Long, Integer>> lst = new ArrayList<>(buckets);
            for (int i = 0; i < buckets; ++i)
                lst.add(F.t(hash(i, part), i));

            Collections.sort(lst, BUCKET_COMPARATOR);
            List<Integer> lstPartClusters = new ArrayList<>(buckets);
            for (IgniteBiTuple<Long, Integer> t : lst)
                lstPartClusters.add(t.get2());

            res.add(lstPartClusters);
        }
        return res;
    }

    /**
     * Gets total number of key partitions. To ensure that all partitions are equally distributed across all nodes,
     * please make sure that this number is significantly larger than a number of nodes. Also, partition size should be
     * relatively small. Try to avoid having partitions with more than quarter million keys. <p> Note that for fully
     * replicated caches this method should always return {@code 1}.
     *
     * @return Total partition count.
     */
    public int getPartitions() {
        return parts;
    }

    /**
     * Sets total number of partitions.
     *
     * @param parts Total number of partitions.
     */
    public void setPartitions(int parts) {
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT, "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);

        this.parts = parts;
    }

    /**
     * Gets hash ID resolver for nodes. This resolver is used to provide
     * alternate hash ID, other than node ID.
     * <p>
     * Node IDs constantly change when nodes get restarted, which causes them to
     * be placed on different locations in the hash ring, and hence causing
     * repartitioning. Providing an alternate hash ID, which survives node restarts,
     * puts node on the same location on the hash ring, hence minimizing required
     * repartitioning.
     *
     * @return Hash ID resolver.
     */
    @Deprecated
    public AffinityNodeHashResolver getHashIdResolver() {
        return hashIdRslvr;
    }

    /**
     * Sets hash ID resolver for nodes. This resolver is used to provide
     * alternate hash ID, other than node ID.
     * <p>
     * Node IDs constantly change when nodes get restarted, which causes them to
     * be placed on different locations in the hash ring, and hence causing
     * repartitioning. Providing an alternate hash ID, which survives node restarts,
     * puts node on the same location on the hash ring, hence minimizing required
     * repartitioning.
     *
     * @param hashIdRslvr Hash ID resolver.
     *
     * @deprecated Use {@link IgniteConfiguration#setConsistentId(Serializable)} instead.
     */
    @Deprecated
    public void setHashIdResolver(AffinityNodeHashResolver hashIdRslvr) {
        this.hashIdRslvr = hashIdRslvr;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, ClusterNode> getBackupFilter() {
        return backupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param backupFilter Optional backup filter.
     */
    public void setBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, List<ClusterNode>> getAffinityBackupFilter() {
        return affinityBackupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param affinityBackupFilter Optional backup filter.
     */
    public void setAffinityBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter) {
        this.affinityBackupFilter = affinityBackupFilter;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public void setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;
    }

    /**
     * Resolves node hash.
     *
     * @param node Cluster node;
     * @return Node hash.
     */
    public Object resolveNodeHash(ClusterNode node) {
        if (hashIdRslvr != null)
            return hashIdRslvr.resolve(node);
        else
            return node.consistentId();
    }

    /**
     * Returns collection of nodes (primary first) for specified partition.
     *
     * @param part partition number.
     * @param nodes Current topology.
     * @param backups Backups count.
     * @param neighborhoodCache Neighborhood cache.
     */
    public List<ClusterNode> assignPartition(int part, List<ClusterNode> nodes, int backups,
        @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
        return assignPartitionMd5Impl(part, nodes, backups, neighborhoodCache);
    }

    /**
     * MD5 based implementation. Used to compatibility with nodes when node's version
     * less then {@link #compatibilityVer}.
     *
     * @param part partition number.
     * @param nodes Current topology.
     * @param backups Backups count.
     * @param neighborhoodCache Neighborhood cache.
     */
    private List<ClusterNode> assignPartitionMd5Impl(int part, List<ClusterNode> nodes, int backups,
        @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
        if (nodes.size() <= 1)
            return nodes;

        List<IgniteBiTuple<Long, ClusterNode>> lst = new ArrayList<>();

        MessageDigest d = digest.get();

        for (ClusterNode node : nodes) {
            Object nodeHash = resolveNodeHash(node);

            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                byte[] nodeHashBytes = ignite.configuration().getMarshaller().marshal(nodeHash);

                out.write(U.intToBytes(part), 0, 4); // Avoid IOException.
                out.write(nodeHashBytes, 0, nodeHashBytes.length); // Avoid IOException.

                d.reset();

                byte[] bytes = d.digest(out.toByteArray());

                long hash =
                    (bytes[0] & 0xFFL)
                        | ((bytes[1] & 0xFFL) << 8)
                        | ((bytes[2] & 0xFFL) << 16)
                        | ((bytes[3] & 0xFFL) << 24)
                        | ((bytes[4] & 0xFFL) << 32)
                        | ((bytes[5] & 0xFFL) << 40)
                        | ((bytes[6] & 0xFFL) << 48)
                        | ((bytes[7] & 0xFFL) << 56);

                lst.add(F.t(hash, node));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        Collections.sort(lst, COMPARATOR);

        int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());

        List<ClusterNode> res = new ArrayList<>(primaryAndBackups);

        ClusterNode primary = lst.get(0).get2();

        res.add(primary);

        // Select backups.
        if (backups > 0) {
            for (int i = 1; i < lst.size() && res.size() < primaryAndBackups; i++) {
                IgniteBiTuple<Long, ClusterNode> next = lst.get(i);

                ClusterNode node = next.get2();

                if (exclNeighbors) {
                    Collection<ClusterNode> allNeighbors = GridCacheUtils.neighborsForNodes(neighborhoodCache, res);

                    if (!allNeighbors.contains(node))
                        res.add(node);
                }
                else if (affinityBackupFilter != null && affinityBackupFilter.apply(node, res))
                    res.add(next.get2());
                else if (backupFilter != null && backupFilter.apply(primary, node))
                    res.add(next.get2());
                else if (affinityBackupFilter == null && backupFilter == null)
                    res.add(next.get2());
            }
        }

        if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
            for (int i = 1; i < lst.size() && res.size() < primaryAndBackups; i++) {
                IgniteBiTuple<Long, ClusterNode> next = lst.get(i);

                ClusterNode node = next.get2();

                if (!res.contains(node))
                    res.add(next.get2());
            }

            if (!exclNeighborsWarn) {
                LT.warn(log, null, "Affinity function excludeNeighbors property is ignored " +
                    "because topology has no enough nodes to assign backups.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= primaryAndBackups;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        return U.safeAbs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        if (useMd5Impl(affCtx))
            return assignPartitionsMd5Impl(affCtx);
        else
            return assignPartitionsWang(affCtx);
    }

    /**
     * MD5 based implementation. Used to compatibility with nodes when node's version
     * less then {@link #compatibilityVer}.
     *
     * @param affCtx Aff context.
     */
    public List<List<ClusterNode>> assignPartitionsMd5Impl(AffinityFunctionContext affCtx) {
        List<List<ClusterNode>> assignments = new ArrayList<>(parts);

        Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
            GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;

        for (int i = 0; i < parts; i++) {
            List<ClusterNode> partAssignment = assignPartitionMd5Impl(i, affCtx.currentTopologySnapshot(), affCtx.backups(),
                neighborhoodCache);

            assignments.add(partAssignment);
        }

        return assignments;
    }

    /**
     * Wang/Jenkins hash with buckets based implementation. Used when all nodes in
     * topology newer then {@link #compatibilityVer}.
     *
     * @param affCtx Aff context.
     */
    private List<List<ClusterNode>> assignPartitionsWang(AffinityFunctionContext affCtx) {
        List<ClusterNode> topSnapshot = affCtx.currentTopologySnapshot();

        Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
            GridCacheUtils.neighbors(topSnapshot) : null;

        List<List<ClusterNode>> assignments = new ArrayList<>(parts);
        for (int i = 0; i < parts; i++) {
            List<ClusterNode> partAssignment = assignPartitionWangImpl(i, topSnapshot, affCtx.backups(),
                neighborhoodCache);

            assignments.add(partAssignment);
        }

        return assignments;
    }

    /**
     /**
     * Wang/Jenkins hash with buckets based implementation. Used when all nodes in
     * topology newer then {@link #compatibilityVer}.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param backups Backups.
     * @param neighborhoodCache Neighborhood cache.
     */
    private List<ClusterNode> assignPartitionWangImpl(int part, List<ClusterNode> nodes, int backups,
        @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
        if (nodes.size() <= 1)
            return nodes;

        Iterable<ClusterNode> sortedNodes;
        if (nodes.size() < DFLT_BUCKETS_COUNT * 2)
            sortedNodes = new FullSortContainer(part, nodes);
        else
            sortedNodes = new BucketSortContainer(part, nodes);

        Iterator<ClusterNode> it = sortedNodes.iterator();

        final int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());

        List<ClusterNode> res = new ArrayList<>(primaryAndBackups);
        Collection<ClusterNode> allNeighbors = new HashSet<>();

        ClusterNode primary = it.next();

        res.add(primary);
        if (exclNeighbors)
            allNeighbors.addAll(neighborhoodCache.get(primary.id()));

        // Select backups.
        if (backups > 0) {
            while (it.hasNext() && res.size() < primaryAndBackups) {
                ClusterNode node = it.next();

                if (exclNeighbors) {
                    if (!allNeighbors.contains(node)) {
                        res.add(node);
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                    }
                }
                else if (backupFilter == null || backupFilter.apply(primary, node)) {
                    res.add(node);
                    if (exclNeighbors)
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                }
            }
        }

        if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
            it = sortedNodes.iterator();
            it.next();
            while (it.hasNext() && res.size() < primaryAndBackups) {

                ClusterNode node = it.next();

                if (!res.contains(node))
                    res.add(node);
            }

            if (!exclNeighborsWarn) {
                LT.warn(log, null, "Affinity function excludeNeighbors property is ignored " +
                    "because topology has no enough nodes to assign backups.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= primaryAndBackups;

        return res;
    }

    /**
     * The switch between MD5 (old) and Wang hash with buckets (new) implementations.
     * If the cluster contains at least one node with version less or equals to #compatibilityVersion
     * the MD5 implementation is used.
     */
    private boolean useMd5Impl(AffinityFunctionContext ctx) {
        return (ctx.oldestNodeVersion() != null) && (compatibilityVer.compareTo(ctx.oldestNodeVersion()) >= 0);
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(parts);
        out.writeBoolean(exclNeighbors);
        out.writeObject(hashIdRslvr);
        out.writeObject(backupFilter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parts = in.readInt();
        exclNeighbors = in.readBoolean();
        hashIdRslvr = (AffinityNodeHashResolver)in.readObject();
        backupFilter = (IgniteBiPredicate<ClusterNode, ClusterNode>)in.readObject();
        bucketIdxsByPart = calculateBucketIdxs(parts, DFLT_BUCKETS_COUNT);
    }

    /**
     *
     */
    private static class HashComparator implements Comparator<IgniteBiTuple<Long, ClusterNode>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(IgniteBiTuple<Long, ClusterNode> o1, IgniteBiTuple<Long, ClusterNode> o2) {
            return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                o1.get2().id().compareTo(o2.get2().id());
        }
    }

    /**
     *
     */
    private static class HashBucketComparator implements Comparator<IgniteBiTuple<Long, Integer>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(IgniteBiTuple<Long, Integer> o1, IgniteBiTuple<Long, Integer> o2) {
            return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                o1.get2().compareTo(o2.get2());
        }
    }

    /**
     * Sort all nodes for specified partition based on hash(part, node.hashCode())
     * Used for small nodes count
     */
    private class FullSortContainer implements Iterable<ClusterNode> {
        /** List of sorted nodes */
        private final List<IgniteBiTuple<Long, ClusterNode>> lst;

        /**
         * @param part Partition number.
         * @param nodes Nodes.
         */
        FullSortContainer(int part, List<ClusterNode> nodes) {
            lst = new ArrayList<>(nodes.size());

            for (ClusterNode node : nodes) {
                IgniteBiTuple<Long, ClusterNode> t = F.t(hash(part, resolveNodeHash(node).hashCode()), node);
                int pos = Collections.binarySearch(lst, t, COMPARATOR);
                lst.add(-1 - pos, t);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override public Iterator<ClusterNode> iterator() {
            return new Itr(lst.iterator());
        }

        /**
         * Iterator implementation
         */
        private class Itr implements Iterator<ClusterNode> {
            /** List iterator. */
            private final Iterator<IgniteBiTuple<Long, ClusterNode>> it;

            /**
             * @param it Iterator.
             */
            Itr(Iterator<IgniteBiTuple<Long, ClusterNode>> it) {
                this.it = it;
            }

            /**
             * {@inheritDoc}
             */
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            /**
             * {@inheritDoc}
             */
            @Override public ClusterNode next() {
                return it.next().get2();
            }

            /**
             * {@inheritDoc}
             */
            @Override public void remove() {
                // No-op
            }
        }

    }

    /**
     * Sort nodes for specified partition based on pre-calculated #bucketIdxsByPartition.
     * Used for large nodes count
     */
    private class BucketSortContainer implements Iterable<ClusterNode> {
        /** Partition number. */
        private final int part;
        /** List of nodes. */
        private final List<ClusterNode> nodes;
        /** Bucket indexes for #part. */
        private final List<Integer> bucketIdxs;
        /** Sorted buckets. It may be useful for second iteration in case there are not enough backup nodes
         * with exclude neighbors.
         */
        private List<List<IgniteBiTuple<Long, ClusterNode>>> buckets = new ArrayList<>();

        BucketSortContainer(int part, List<ClusterNode> nodes) {
            this.part = part;
            this.nodes = nodes;
            bucketIdxs = bucketIdxsByPart.get(part);
        }

        /** {@inheritDoc} */
        @Override public Iterator<ClusterNode> iterator() {
            return new Itr();
        }

        /**
         * Iterator implementation
         */
        private class Itr implements Iterator<ClusterNode> {
            /** Current bucket. */
            private List<IgniteBiTuple<Long, ClusterNode>> currBucket;
            /** Current bucket index. */
            private int currBucketIdx;
            /** Current node index. */
            private int currNodeIdx;

            /**
             * Default constructor.
             */
            Itr() {
                prepareNewBucket();
            }

            /**
             * Sort the next bucket of nodes and reset node index
             */
            private void prepareNewBucket() {
                if(buckets.size() <= currBucketIdx) {
                    int bucketIdx = bucketIdxs.get(currBucketIdx);
                    int bucketSize = nodes.size() / DFLT_BUCKETS_COUNT +
                        ((bucketIdx < (nodes.size() % DFLT_BUCKETS_COUNT)) ? 1 : 0);

                    int begin = bucketIdx * (nodes.size() / DFLT_BUCKETS_COUNT) +
                        ((bucketIdx < (nodes.size() % DFLT_BUCKETS_COUNT)) ? bucketIdx
                            : nodes.size() % DFLT_BUCKETS_COUNT);

                    int end = begin + bucketSize;
                    currBucket = new ArrayList<>(bucketSize);

                    for (int i = begin; i < end; ++i) {
                        ClusterNode node = nodes.get(i);
                        IgniteBiTuple<Long, ClusterNode> t = F.t(hash(resolveNodeHash(node).hashCode(), part), node);
                        int pos = Collections.binarySearch(currBucket, t, COMPARATOR);
                        currBucket.add(-1 - pos, t);
                    }
                    buckets.add(currBucket);
                } else
                    currBucket = buckets.get(currBucketIdx);

                currNodeIdx = 0;
            }

            /**
             * {@inheritDoc}
             */
            @Override public boolean hasNext() {
                return currBucketIdx < bucketIdxs.size() - 1 || currNodeIdx < currBucket.size() - 1;
            }

            /**
             * {@inheritDoc}
             */
            @Override public ClusterNode next() {
                ClusterNode node = currBucket.get(currNodeIdx).get2();
                ++currNodeIdx;
                if (currNodeIdx == currBucket.size()) {
                    ++currBucketIdx;
                    if (currBucketIdx == bucketIdxs.size())
                        throw new NoSuchElementException();

                    prepareNewBucket();
                }
                return node;
            }

            /**
             * {@inheritDoc}
             */
            @Override public void remove() {
                // No-op
            }
        }
    }
}
