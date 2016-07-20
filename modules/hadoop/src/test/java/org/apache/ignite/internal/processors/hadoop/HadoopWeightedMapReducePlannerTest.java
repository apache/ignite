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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopWeightedMapReducePlanner;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopAbstractMapReducePlanner;
import org.apache.ignite.internal.processors.igfs.IgfsIgniteMock;
import org.apache.ignite.internal.processors.igfs.IgfsMock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Tests for weighted map-reduce planned.
 */
public class HadoopWeightedMapReducePlannerTest extends GridCommonAbstractTest {
    /** ID 1. */
    private static final UUID ID_1 = new UUID(0, 1);

    /** ID 2. */
    private static final UUID ID_2 = new UUID(0, 2);

    /** ID 3. */
    private static final UUID ID_3 = new UUID(0, 3);

    /** MAC 1. */
    private static final String MAC_1 = "mac1";

    /** MAC 2. */
    private static final String MAC_2 = "mac2";

    /** MAC 3. */
    private static final String MAC_3 = "mac3";

    /** Host 1. */
    private static final String HOST_1 = "host1";

    /** Host 2. */
    private static final String HOST_2 = "host2";

    /** Host 3. */
    private static final String HOST_3 = "host3";

    /** Host 4. */
    private static final String HOST_4 = "host4";

    /** Host 5. */
    private static final String HOST_5 = "host5";

    /** Standard node 1. */
    private static final MockNode NODE_1 = new MockNode(ID_1, MAC_1, HOST_1);

    /** Standard node 2. */
    private static final MockNode NODE_2 = new MockNode(ID_2, MAC_2, HOST_2);

    /** Standard node 3. */
    private static final MockNode NODE_3 = new MockNode(ID_3, MAC_3, HOST_3);

    /** Standard nodes. */
    private static final Collection<ClusterNode> NODES;

    /**
     * Static initializer.
     */
    static {
        NODES = new ArrayList<>();

        NODES.add(NODE_1);
        NODES.add(NODE_2);
        NODES.add(NODE_3);
    }

    /**
     * Test one IGFS split being assigned to affinity node.
     *
     * @throws Exception If failed.
     */
    public void testOneIgfsSplitAffinity() throws Exception {
        IgfsMock igfs = LocationsBuilder.create().add(0, NODE_1).add(50, NODE_2).add(100, NODE_3).buildIgfs();

        List<HadoopInputSplit> splits = new ArrayList<>();

        splits.add(new HadoopFileBlock(new String[] { HOST_1 }, URI.create("igfs://igfs@/file"), 0, 50));

        final int expReducers = 4;

        HadoopPlannerMockJob job = new HadoopPlannerMockJob(splits, expReducers);

        IgniteHadoopWeightedMapReducePlanner planner = createPlanner(igfs);

        HadoopMapReducePlan plan = planner.preparePlan(job, NODES, null);

        assert plan.mappers() == 1;
        assert plan.mapperNodeIds().size() == 1;
        assert plan.mapperNodeIds().contains(ID_1);

        checkPlanMappers(plan, splits, NODES, false/*only 1 split*/);
        checkPlanReducers(plan, NODES, expReducers, false/* because of threshold behavior.*/);
    }

    /**
     * Test one HDFS splits.
     *
     * @throws Exception If failed.
     */
    public void testHdfsSplitsAffinity() throws Exception {
        IgfsMock igfs = LocationsBuilder.create().add(0, NODE_1).add(50, NODE_2).add(100, NODE_3).buildIgfs();

        final List<HadoopInputSplit> splits = new ArrayList<>();

        splits.add(new HadoopFileBlock(new String[] { HOST_1 }, URI.create("hfds://" + HOST_1 + "/x"), 0, 50));
        splits.add(new HadoopFileBlock(new String[] { HOST_2 }, URI.create("hfds://" + HOST_2 + "/x"), 50, 100));
        splits.add(new HadoopFileBlock(new String[] { HOST_3 }, URI.create("hfds://" + HOST_3 + "/x"), 100, 37));

        // The following splits belong to hosts that are out of Ignite topology at all.
        // This means that these splits should be assigned to any least loaded modes:
        splits.add(new HadoopFileBlock(new String[] { HOST_4 }, URI.create("hfds://" + HOST_4 + "/x"), 138, 2));
        splits.add(new HadoopFileBlock(new String[] { HOST_5 }, URI.create("hfds://" + HOST_5 + "/x"), 140, 3));

        final int expReducers = 7;

        HadoopPlannerMockJob job = new HadoopPlannerMockJob(splits, expReducers);

        IgniteHadoopWeightedMapReducePlanner planner = createPlanner(igfs);

        final HadoopMapReducePlan plan = planner.preparePlan(job, NODES, null);

        checkPlanMappers(plan, splits, NODES, true);

        checkPlanReducers(plan, NODES, expReducers, true);
    }

    /**
     * Test HDFS splits with Replication == 3.
     *
     * @throws Exception If failed.
     */
    public void testHdfsSplitsReplication() throws Exception {
        IgfsMock igfs = LocationsBuilder.create().add(0, NODE_1).add(50, NODE_2).add(100, NODE_3).buildIgfs();

        final List<HadoopInputSplit> splits = new ArrayList<>();

        splits.add(new HadoopFileBlock(new String[] { HOST_1, HOST_2, HOST_3 }, URI.create("hfds://" + HOST_1 + "/x"), 0, 50));
        splits.add(new HadoopFileBlock(new String[] { HOST_2, HOST_3, HOST_4 }, URI.create("hfds://" + HOST_2 + "/x"), 50, 100));
        splits.add(new HadoopFileBlock(new String[] { HOST_3, HOST_4, HOST_5 }, URI.create("hfds://" + HOST_3 + "/x"), 100, 37));
        // The following splits belong to hosts that are out of Ignite topology at all.
        // This means that these splits should be assigned to any least loaded modes:
        splits.add(new HadoopFileBlock(new String[] { HOST_4, HOST_5, HOST_1 }, URI.create("hfds://" + HOST_4 + "/x"), 138, 2));
        splits.add(new HadoopFileBlock(new String[] { HOST_5, HOST_1, HOST_2 }, URI.create("hfds://" + HOST_5 + "/x"), 140, 3));

        final int expReducers = 8;

        HadoopPlannerMockJob job = new HadoopPlannerMockJob(splits, expReducers);

        IgniteHadoopWeightedMapReducePlanner planner = createPlanner(igfs);

        final HadoopMapReducePlan plan = planner.preparePlan(job, NODES, null);

        checkPlanMappers(plan, splits, NODES, true);

        checkPlanReducers(plan, NODES, expReducers, true);
    }

    /**
     * Get all IDs.
     *
     * @param nodes Nodes.
     * @return IDs.
     */
    private static Set<UUID> allIds(Collection<ClusterNode> nodes) {
        Set<UUID> allIds = new HashSet<>();

        for (ClusterNode n : nodes)
            allIds.add(n.id());

        return allIds;
    }

    /**
     * Check mappers for the plan.
     *
     * @param plan Plan.
     * @param splits Splits.
     * @param nodes Nodes.
     * @param expectUniformity WHether uniformity is expected.
     */
    private static void checkPlanMappers(HadoopMapReducePlan plan, List<HadoopInputSplit> splits,
        Collection<ClusterNode> nodes, boolean expectUniformity) {
        // Number of mappers should correspomd to the number of input splits:
        assertEquals(splits.size(), plan.mappers());

        if (expectUniformity) {
            // mappers are assigned to all available nodes:
            assertEquals(nodes.size(), plan.mapperNodeIds().size());


            assertEquals(allIds(nodes), plan.mapperNodeIds());
        }

        // Check all splits are covered by mappers:
        Set<HadoopInputSplit> set = new HashSet<>();

        for (UUID id: plan.mapperNodeIds()) {
            Collection<HadoopInputSplit> sp = plan.mappers(id);

            assert sp != null;

            for (HadoopInputSplit s: sp)
                assertTrue(set.add(s));
        }

        // must be of the same size & contain same elements:
        assertEquals(set, new HashSet<>(splits));
    }

    /**
     * Check plan reducers.
     *
     * @param plan Plan.
     * @param nodes Nodes.
     * @param expReducers Expected reducers.
     * @param expectUniformity Expected uniformity.
     */
    private static void checkPlanReducers(HadoopMapReducePlan plan,
        Collection<ClusterNode> nodes, int expReducers, boolean expectUniformity) {

        assertEquals(expReducers, plan.reducers());

        if (expectUniformity)
            assertEquals(allIds(nodes), plan.reducerNodeIds());

        int sum = 0;
        int lenSum = 0;

        for (UUID uuid: plan.reducerNodeIds()) {
            int[] rr = plan.reducers(uuid);

            assert rr != null;

            lenSum += rr.length;

            for (int i: rr)
                sum += i;
        }

        assertEquals(expReducers, lenSum);

        // Numbers in the arrays must be consequtive integers stating from 0,
        // check that simply calculating their total sum:
        assertEquals((lenSum * (lenSum - 1) / 2), sum);
    }

    /**
     * Create planner for IGFS.
     *
     * @param igfs IGFS.
     * @return Planner.
     */
    private static IgniteHadoopWeightedMapReducePlanner createPlanner(IgfsMock igfs) {
        IgniteHadoopWeightedMapReducePlanner planner = new IgniteHadoopWeightedMapReducePlanner();

        IgfsIgniteMock ignite = new IgfsIgniteMock(null, igfs);

        GridTestUtils.setFieldValue(planner, HadoopAbstractMapReducePlanner.class, "ignite", ignite);

        return planner;
    }

    /**
     * Throw {@link UnsupportedOperationException}.
     */
    private static void throwUnsupported() {
        throw new UnsupportedOperationException("Should not be called!");
    }

    /**
     * Mocked node.
     */
    private static class MockNode implements ClusterNode {
        /** ID. */
        private final UUID id;

        /** MAC addresses. */
        private final String macs;

        /** Addresses. */
        private final List<String> addrs;

        /**
         * Constructor.
         *
         * @param id Node ID.
         * @param macs MAC addresses.
         * @param addrs Addresses.
         */
        public MockNode(UUID id, String macs, String... addrs) {
            assert addrs != null;

            this.id = id;
            this.macs = macs;

            this.addrs = Arrays.asList(addrs);
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public <T> T attribute(String name) {
            if (F.eq(name, IgniteNodeAttributes.ATTR_MACS))
                return (T)macs;

            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> addresses() {
            return addrs;
        }

        /** {@inheritDoc} */
        @Override public Object consistentId() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterMetrics metrics() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> attributes() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> hostNames() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public long order() {
            throwUnsupported();

            return 0;
        }

        /** {@inheritDoc} */
        @Override public IgniteProductVersion version() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isLocal() {
            throwUnsupported();

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDaemon() {
            throwUnsupported();

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isClient() {
            throwUnsupported();

            return false;
        }
    }

    /**
     * Locations builder.
     */
    private static class LocationsBuilder {
        /** Locations. */
        private final TreeMap<Long, Collection<MockNode>> locs = new TreeMap<>();

        /**
         * Create new locations builder.
         *
         * @return Locations builder.
         */
        public static LocationsBuilder create() {
            return new LocationsBuilder();
        }

        /**
         * Add locations.
         *
         * @param start Start.
         * @param nodes Nodes.
         * @return This builder for chaining.
         */
        public LocationsBuilder add(long start, MockNode... nodes) {
            locs.put(start, Arrays.asList(nodes));

            return this;
        }

        /**
         * Build locations.
         *
         * @return Locations.
         */
        public TreeMap<Long, Collection<MockNode>> build() {
            return locs;
        }

        /**
         * Build IGFS.
         *
         * @return IGFS.
         */
        public MockIgfs buildIgfs() {
            return new MockIgfs(build());
        }
    }

    /**
     * Mocked IGFS.
     */
    private static class MockIgfs extends IgfsMock {
        /** Block locations. */
        private final TreeMap<Long, Collection<MockNode>> locs;

        /**
         * Constructor.
         *
         * @param locs Block locations.
         */
        public MockIgfs(TreeMap<Long, Collection<MockNode>> locs) {
            super("igfs");

            this.locs = locs;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) {
            Collection<IgfsBlockLocation> res = new ArrayList<>();

            long cur = start;
            long remaining = len;

            long prevLocStart = -1;
            Collection<MockNode> prevLocNodes = null;

            for (Map.Entry<Long, Collection<MockNode>> locEntry : locs.entrySet()) {
                long locStart = locEntry.getKey();
                Collection<MockNode> locNodes = locEntry.getValue();

                if (prevLocNodes != null) {
                    if (cur < locStart) {
                        // Add part from previous block.
                        long prevLen = locStart - prevLocStart;

                        res.add(new IgfsBlockLocationMock(cur, prevLen, prevLocNodes));

                        cur = locStart;
                        remaining -= prevLen;
                    }
                }

                prevLocStart = locStart;
                prevLocNodes = locNodes;

                if (remaining == 0)
                    break;
            }

            // Add remainder.
            if (remaining != 0)
                res.add(new IgfsBlockLocationMock(cur, remaining, prevLocNodes));

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean exists(IgfsPath path) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isProxy(URI path) {
            return false;
        }
    }

    /**
     * Mocked block location.
     */
    private static class IgfsBlockLocationMock implements IgfsBlockLocation {
        /** Start. */
        private final long start;

        /** Length. */
        private final long len;

        /** Node IDs. */
        private final List<UUID> nodeIds;

        /**
         * Constructor.
         *
         * @param start Start.
         * @param len Length.
         * @param nodes Nodes.
         */
        public IgfsBlockLocationMock(long start, long len, Collection<MockNode> nodes) {
            this.start = start;
            this.len = len;

            this.nodeIds = new ArrayList<>(nodes.size());

            for (MockNode node : nodes)
                nodeIds.add(node.id);
        }

        /** {@inheritDoc} */
        @Override public long start() {
            return start;
        }

        /** {@inheritDoc} */
        @Override public long length() {
            return len;
        }

        /** {@inheritDoc} */
        @Override public Collection<UUID> nodeIds() {
            return nodeIds;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> names() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> hosts() {
            throwUnsupported();

            return null;
        }
    }
}
