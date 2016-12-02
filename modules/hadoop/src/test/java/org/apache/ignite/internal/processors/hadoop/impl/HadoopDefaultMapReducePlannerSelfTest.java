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

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopMapReducePlanner;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopAbstractMapReducePlanner;
import org.apache.ignite.internal.processors.igfs.IgfsBlockLocationImpl;
import org.apache.ignite.internal.processors.igfs.IgfsIgniteMock;
import org.apache.ignite.internal.processors.igfs.IgfsMock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class HadoopDefaultMapReducePlannerSelfTest extends HadoopAbstractSelfTest {
    /** */
    private static final UUID ID_1 = new UUID(0, 1);

    /** */
    private static final UUID ID_2 = new UUID(0, 2);

    /** */
    private static final UUID ID_3 = new UUID(0, 3);

    /** */
    private static final String HOST_1 = "host1";

    /** */
    private static final String HOST_2 = "host2";

    /** */
    private static final String HOST_3 = "host3";

    /** */
    private static final String INVALID_HOST_1 = "invalid_host1";

    /** */
    private static final String INVALID_HOST_2 = "invalid_host2";

    /** */
    private static final String INVALID_HOST_3 = "invalid_host3";

    /** Mocked IGFS. */
    private static final IgniteFileSystem IGFS = new MockIgfs();

    /** Mocked Grid. */
    private static final IgfsIgniteMock GRID = new IgfsIgniteMock(null, IGFS);

    /** Planner. */
    private static final HadoopMapReducePlanner PLANNER = new IgniteHadoopMapReducePlanner();

    /** Block locations. */
    private static final Map<Block, Collection<IgfsBlockLocation>> BLOCK_MAP = new HashMap<>();

    /** Proxy map. */
    private static final Map<URI, Boolean> PROXY_MAP = new HashMap<>();

    /** Last created plan. */
    private static final ThreadLocal<HadoopMapReducePlan> PLAN = new ThreadLocal<>();

    /**
     * Static initializer.
     */
    static {
        GridTestUtils.setFieldValue(PLANNER, HadoopAbstractMapReducePlanner.class, "ignite", GRID);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridTestUtils.setFieldValue(PLANNER, HadoopAbstractMapReducePlanner.class, "log", log());

        BLOCK_MAP.clear();
        PROXY_MAP.clear();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testIgfsOneBlockPerNode() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1);
        HadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_2);
        HadoopFileBlock split3 = split(true, "/file3", 0, 100, HOST_3);

        mapIgfsBlock(split1.file(), 0, 100, location(0, 100, ID_1));
        mapIgfsBlock(split2.file(), 0, 100, location(0, 100, ID_2));
        mapIgfsBlock(split3.file(), 0, 100, location(0, 100, ID_3));

        plan(1, split1);
        assert ensureMappers(ID_1, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureEmpty(ID_2);
        assert ensureEmpty(ID_3);

        plan(2, split1);
        assert ensureMappers(ID_1, split1);
        assert ensureReducers(ID_1, 2);
        assert ensureEmpty(ID_2);
        assert ensureEmpty(ID_3);

        plan(1, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0) || ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) || ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2, split3);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureMappers(ID_3, split3);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureReducers(ID_3, 1);

        plan(5, split1, split2, split3);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureMappers(ID_3, split3);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testNonIgfsOneBlockPerNode() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(false, "/file1", 0, 100, HOST_1);
        HadoopFileBlock split2 = split(false, "/file2", 0, 100, HOST_2);
        HadoopFileBlock split3 = split(false, "/file3", 0, 100, HOST_3);

        plan(1, split1);
        assert ensureMappers(ID_1, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureEmpty(ID_2);
        assert ensureEmpty(ID_3);

        plan(2, split1);
        assert ensureMappers(ID_1, split1);
        assert ensureReducers(ID_1, 2);
        assert ensureEmpty(ID_2);
        assert ensureEmpty(ID_3);

        plan(1, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0) || ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) || ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2, split3);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureMappers(ID_3, split3);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureReducers(ID_3, 1);

        plan(5, split1, split2, split3);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureMappers(ID_3, split3);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testIgfsSeveralBlocksPerNode() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1, HOST_2);
        HadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_1, HOST_2);
        HadoopFileBlock split3 = split(true, "/file3", 0, 100, HOST_1, HOST_3);

        mapIgfsBlock(split1.file(), 0, 100, location(0, 100, ID_1, ID_2));
        mapIgfsBlock(split2.file(), 0, 100, location(0, 100, ID_1, ID_2));
        mapIgfsBlock(split3.file(), 0, 100, location(0, 100, ID_1, ID_3));

        plan(1, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 1) && ensureEmpty(ID_2) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 2) && ensureEmpty(ID_2) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 2);
        assert ensureEmpty(ID_3);

        plan(1, split1, split2);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0) || ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1, split2);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2, split3);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureReducers(ID_3, 1);

        plan(5, split1, split2, split3);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testNonIgfsSeveralBlocksPerNode() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(false, "/file1", 0, 100, HOST_1, HOST_2);
        HadoopFileBlock split2 = split(false, "/file2", 0, 100, HOST_1, HOST_2);
        HadoopFileBlock split3 = split(false, "/file3", 0, 100, HOST_1, HOST_3);

        plan(1, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 1) && ensureEmpty(ID_2) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 2) && ensureEmpty(ID_2) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 2);
        assert ensureEmpty(ID_3);

        plan(1, split1, split2);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0) || ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(2, split1, split2);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);

        plan(3, split1, split2, split3);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureReducers(ID_3, 1);

        plan(5, split1, split2, split3);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testIgfsSeveralComplexBlocksPerNode() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1, HOST_2, HOST_3);
        HadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_1, HOST_2, HOST_3);

        mapIgfsBlock(split1.file(), 0, 100, location(0, 50, ID_1, ID_2), location(51, 100, ID_1, ID_3));
        mapIgfsBlock(split2.file(), 0, 100, location(0, 50, ID_1, ID_2), location(51, 100, ID_2, ID_3));

        plan(1, split1);
        assert ensureMappers(ID_1, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureEmpty(ID_2);
        assert ensureEmpty(ID_3);

        plan(1, split2);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_1);
        assert ensureEmpty(ID_3);

        plan(1, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1) || ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0);
        assert ensureEmpty(ID_3);

        plan(2, split1, split2);
        assert ensureMappers(ID_1, split1);
        assert ensureMappers(ID_2, split2);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureEmpty(ID_3);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testNonIgfsOrphans() throws IgniteCheckedException {
        HadoopFileBlock split1 = split(false, "/file1", 0, 100, INVALID_HOST_1, INVALID_HOST_2);
        HadoopFileBlock split2 = split(false, "/file2", 0, 100, INVALID_HOST_1, INVALID_HOST_3);
        HadoopFileBlock split3 = split(false, "/file3", 0, 100, INVALID_HOST_2, INVALID_HOST_3);

        plan(1, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 1) && ensureEmpty(ID_2) && ensureEmpty(ID_3) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 1) && ensureEmpty(ID_3) ||
            ensureEmpty(ID_1) && ensureEmpty(ID_2) && ensureMappers(ID_3, split1) && ensureReducers(ID_3, 1);

        plan(2, split1);
        assert ensureMappers(ID_1, split1) && ensureReducers(ID_1, 2) && ensureEmpty(ID_2) && ensureEmpty(ID_3) ||
            ensureEmpty(ID_1) && ensureMappers(ID_2, split1) && ensureReducers(ID_2, 2) && ensureEmpty(ID_3) ||
            ensureEmpty(ID_1) && ensureEmpty(ID_2) && ensureMappers(ID_3, split1) && ensureReducers(ID_3, 2);

        plan(1, split1, split2, split3);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split1) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split1) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split1);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 0) && ensureReducers(ID_3, 0) ||
            ensureReducers(ID_1, 0) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 0) ||
            ensureReducers(ID_1, 0) && ensureReducers(ID_2, 0) && ensureReducers(ID_3, 1);

        plan(3, split1, split2, split3);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split1) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split1) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split1);
        assert ensureReducers(ID_1, 1);
        assert ensureReducers(ID_2, 1);
        assert ensureReducers(ID_3, 1);

        plan(5, split1, split2, split3);
        assert ensureMappers(ID_1, split1) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split1) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split3) ||
            ensureMappers(ID_1, split2) && ensureMappers(ID_2, split3) && ensureMappers(ID_3, split1) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split1) && ensureMappers(ID_3, split2) ||
            ensureMappers(ID_1, split3) && ensureMappers(ID_2, split2) && ensureMappers(ID_3, split1);
        assert ensureReducers(ID_1, 1) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 1) && ensureReducers(ID_3, 2) ||
            ensureReducers(ID_1, 2) && ensureReducers(ID_2, 2) && ensureReducers(ID_3, 1);
    }

    /**
     * Create plan.
     *
     * @param reducers Reducers count.
     * @param splits Splits.
     * @return Plan.
     * @throws IgniteCheckedException If failed.
     */
    private static HadoopMapReducePlan plan(int reducers, HadoopInputSplit... splits) throws IgniteCheckedException {
        assert reducers > 0;
        assert splits != null && splits.length > 0;

        Collection<HadoopInputSplit> splitList = new ArrayList<>(splits.length);

        Collections.addAll(splitList, splits);

        Collection<ClusterNode> top = new ArrayList<>();

        GridTestNode node1 = new GridTestNode(ID_1);
        GridTestNode node2 = new GridTestNode(ID_2);
        GridTestNode node3 = new GridTestNode(ID_3);

        node1.setHostName(HOST_1);
        node2.setHostName(HOST_2);
        node3.setHostName(HOST_3);

        top.add(node1);
        top.add(node2);
        top.add(node3);

        HadoopMapReducePlan plan = PLANNER.preparePlan(new HadoopPlannerMockJob(splitList, reducers), top, null);

        PLAN.set(plan);

        return plan;
    }

    /**
     * Ensure that node contains the given mappers.
     *
     * @param nodeId Node ID.
     * @param expSplits Expected splits.
     * @return {@code True} if this assumption is valid.
     */
    private static boolean ensureMappers(UUID nodeId, HadoopInputSplit... expSplits) {
        Collection<HadoopInputSplit> expSplitsCol = new ArrayList<>();

        Collections.addAll(expSplitsCol, expSplits);

        Collection<HadoopInputSplit> splits = PLAN.get().mappers(nodeId);

        return F.eq(expSplitsCol, splits);
    }

    /**
     * Ensure that node contains the given amount of reducers.
     *
     * @param nodeId Node ID.
     * @param reducers Reducers.
     * @return {@code True} if this assumption is valid.
     */
    private static boolean ensureReducers(UUID nodeId, int reducers) {
        int[] reducersArr = PLAN.get().reducers(nodeId);

        return reducers == 0 ? F.isEmpty(reducersArr) : (reducersArr != null && reducersArr.length == reducers);
    }

    /**
     * Ensure that no mappers and reducers is located on this node.
     *
     * @param nodeId Node ID.
     * @return {@code True} if this assumption is valid.
     */
    private static boolean ensureEmpty(UUID nodeId) {
        return F.isEmpty(PLAN.get().mappers(nodeId)) && F.isEmpty(PLAN.get().reducers(nodeId));
    }

    /**
     * Create split.
     *
     * @param igfs IGFS flag.
     * @param file File.
     * @param start Start.
     * @param len Length.
     * @param hosts Hosts.
     * @return Split.
     */
    private static HadoopFileBlock split(boolean igfs, String file, long start, long len, String... hosts) {
        URI uri = URI.create((igfs ? "igfs://igfs@" : "hdfs://") + file);

        return new HadoopFileBlock(hosts, uri, start, len);
    }

    /**
     * Create block location.
     *
     * @param start Start.
     * @param len Length.
     * @param nodeIds Node IDs.
     * @return Block location.
     */
    private static IgfsBlockLocation location(long start, long len, UUID... nodeIds) {
        assert nodeIds != null && nodeIds.length > 0;

        Collection<ClusterNode> nodes = new ArrayList<>(nodeIds.length);

        for (UUID id : nodeIds)
            nodes.add(new GridTestNode(id));

        return new IgfsBlockLocationImpl(start, len, nodes);
    }

    /**
     * Map IGFS block to nodes.
     *
     * @param file File.
     * @param start Start.
     * @param len Length.
     * @param locations Locations.
     */
    private static void mapIgfsBlock(URI file, long start, long len, IgfsBlockLocation... locations) {
        assert locations != null && locations.length > 0;

        IgfsPath path = new IgfsPath(file);

        Block block = new Block(path, start, len);

        Collection<IgfsBlockLocation> locationsList = new ArrayList<>();

        Collections.addAll(locationsList, locations);

        BLOCK_MAP.put(block, locationsList);
    }

    /**
     * Block.
     */
    private static class Block {
        /** */
        private final IgfsPath path;

        /** */
        private final long start;

        /** */
        private final long len;

        /**
         * Constructor.
         *
         * @param path Path.
         * @param start Start.
         * @param len Length.
         */
        private Block(IgfsPath path, long start, long len) {
            this.path = path;
            this.start = start;
            this.len = len;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Block)) return false;

            Block block = (Block) o;

            if (len != block.len)
                return false;

            if (start != block.start)
                return false;

            if (!path.equals(block.path))
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = path.hashCode();

            res = 31 * res + (int) (start ^ (start >>> 32));
            res = 31 * res + (int) (len ^ (len >>> 32));

            return res;
        }
    }

    /**
     * Mocked IGFS.
     */
    private static class MockIgfs extends IgfsMock {
        /**
         * Constructor.
         */
        public MockIgfs() {
            super("igfs");
        }

        /** {@inheritDoc} */
        @Override public boolean isProxy(URI path) {
            return PROXY_MAP.containsKey(path) && PROXY_MAP.get(path);
        }

        /** {@inheritDoc} */
        @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) {
            return BLOCK_MAP.get(new Block(path, start, len));
        }

        /** {@inheritDoc} */
        @Override public boolean exists(IgfsPath path) {
            return true;
        }
    }
}