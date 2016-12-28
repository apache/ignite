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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Affinity API tests.
 */
public class GridCacheAffinityApiSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Affinity.
     */
    private AffinityFunction affinity() {
        return ((IgniteKernal)grid(0)).internalCache().configuration().getAffinity();
    }

    /**
     * @return Affinity mapper.
     */
    private AffinityKeyMapper affinityMapper() {
        return ((IgniteKernal)grid(0)).internalCache().configuration().getAffinityMapper();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPartitions() throws Exception {
        assertEquals(affinity().partitions(), grid(0).affinity(null).partitions());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPartition() throws Exception {
        String key = "key";

        assertEquals(affinity().partition(key), grid(0).affinity(null).partition(key));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryPartitionsOneNode() throws Exception {
        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        List<List<ClusterNode>> assignment = affinity().assignPartitions(ctx);

        for (ClusterNode node : grid(0).cluster().nodes()) {
            int[] parts = grid(0).affinity(null).primaryPartitions(node);

            assert !F.isEmpty(parts);

            for (int p : parts) {
                Collection<ClusterNode> owners = nodes(assignment, p);

                assert !F.isEmpty(owners);

                ClusterNode primary = F.first(owners);

                assert F.eqNodes(node, primary);
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryPartitions() throws Exception {
        // Pick 2 nodes and create a projection over them.
        ClusterNode n0 = grid(0).localNode();

        int[] parts = grid(0).affinity(null).primaryPartitions(n0);

        info("Primary partitions count: " + parts.length);

        assert parts.length > 1 : "Invalid partitions: " + Arrays.toString(parts);

        for (int part : parts)
            assert part >= 0;

        assert !F.isEmpty(parts);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        List<List<ClusterNode>> assignment = affinity().assignPartitions(ctx);

        for (int p : parts) {
            Collection<ClusterNode> owners = nodes(assignment, p);

            assert !F.isEmpty(owners);

            ClusterNode primary = F.first(owners);

            assert F.eqNodes(n0, primary);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testBackupPartitions() throws Exception {
        // Pick 2 nodes and create a projection over them.
        ClusterNode n0 = grid(0).localNode();

        // Get backup partitions without explicitly specified levels.
        int[] parts = grid(0).affinity(null).backupPartitions(n0);

        assert !F.isEmpty(parts);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        List<List<ClusterNode>> assignment = affinity().assignPartitions(ctx);

        for (int p : parts) {
            Collection<ClusterNode> owners = new ArrayList<>(nodes(assignment, p));

            assert !F.isEmpty(owners);

            // Remove primary.
            Iterator<ClusterNode> iter = owners.iterator();

            iter.next();
            iter.remove();

            assert owners.contains(n0);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAllPartitions() throws Exception {
        // Pick 2 nodes and create a projection over them.
        ClusterNode n0 = grid(0).localNode();

        int[] parts = grid(0).affinity(null).allPartitions(n0);

        assert !F.isEmpty(parts);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        List<List<ClusterNode>> assignment = affinity().assignPartitions(ctx);

        for (int p : parts) {
            Collection<ClusterNode> owners = nodes(assignment, p);

            assert !F.isEmpty(owners);

            assert owners.contains(n0);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapPartitionToNode() throws Exception {
        int part = RND.nextInt(affinity().partitions());

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        AffinityFunction aff = affinity();

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        assertEquals(F.first(nodes(assignment, aff, part)), grid(0).affinity(null).mapPartitionToNode(part));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapPartitionsToNode() throws Exception {
        Map<Integer, ClusterNode> map = grid(0).affinity(null).mapPartitionsToNodes(F.asList(0, 1, 5, 19, 12));

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        AffinityFunction aff = affinity();

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        for (Map.Entry<Integer, ClusterNode> e : map.entrySet())
            assert F.eqNodes(F.first(nodes(assignment, aff, e.getKey())), e.getValue());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapPartitionsToNodeArray() throws Exception {
        Map<Integer, ClusterNode> map = grid(0).affinity(null).mapPartitionsToNodes(F.asList(0, 1, 5, 19, 12));

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        AffinityFunction aff = affinity();

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        for (Map.Entry<Integer, ClusterNode> e : map.entrySet())
            assert F.eqNodes(F.first(nodes(assignment, aff, e.getKey())), e.getValue());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapPartitionsToNodeCollection() throws Exception {
        Collection<Integer> parts = new LinkedList<>();

        for (int p = 0; p < affinity().partitions(); p++)
            parts.add(p);

        Map<Integer, ClusterNode> map = grid(0).affinity(null).mapPartitionsToNodes(parts);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(grid(0).cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        AffinityFunction aff = affinity();

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        for (Map.Entry<Integer, ClusterNode> e : map.entrySet())
            assert F.eqNodes(F.first(nodes(assignment, aff, e.getKey())), e.getValue());
    }

    /**
     * Gets affinity nodes for partition.
     *
     * @param assignments Assignments.
     * @param part Partition to get affinity nodes for.
     * @return Affinity nodes.
     */
    private List<ClusterNode> nodes(List<List<ClusterNode>> assignments, int part) {
        return assignments.get(part);
    }

    /**
     * Gets affinity nodes for partition.
     *
     * @param key Affinity key.
     * @return Affinity nodes.
     */
    private Iterable<ClusterNode> nodes(List<List<ClusterNode>> assignment, AffinityFunction aff, Object key) {
        return assignment.get(aff.partition(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionWithAffinityMapper() throws Exception {
        AffinityKey<Integer> key = new AffinityKey<>(1, 2);

        int expPart = affinity().partition(affinityMapper().affinityKey(key));

        for (int i = 0; i < gridCount(); i++)
            assertEquals(expPart, grid(i).affinity(null).partition(key));
    }
}