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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.ignitefs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.processors.hadoop.planner.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public class GridHadoopDefaultMapReducePlannerSelfTest extends GridHadoopAbstractSelfTest {
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

    /** Mocked Grid. */
    private static final MockIgnite GRID = new MockIgnite();

    /** Mocked GGFS. */
    private static final IgniteFs GGFS = new MockGgfs();

    /** Planner. */
    private static final GridHadoopMapReducePlanner PLANNER = new GridHadoopDefaultMapReducePlanner();

    /** Block locations. */
    private static final Map<Block, Collection<IgniteFsBlockLocation>> BLOCK_MAP = new HashMap<>();

    /** Proxy map. */
    private static final Map<URI, Boolean> PROXY_MAP = new HashMap<>();

    /** Last created plan. */
    private static final ThreadLocal<GridHadoopMapReducePlan> PLAN = new ThreadLocal<>();

    /**
     *
     */
    static {
        GridTestUtils.setFieldValue(PLANNER, "ignite", GRID);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridTestUtils.setFieldValue(PLANNER, "log", log());

        BLOCK_MAP.clear();
        PROXY_MAP.clear();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testGgfsOneBlockPerNode() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1);
        GridHadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_2);
        GridHadoopFileBlock split3 = split(true, "/file3", 0, 100, HOST_3);

        mapGgfsBlock(split1.file(), 0, 100, location(0, 100, ID_1));
        mapGgfsBlock(split2.file(), 0, 100, location(0, 100, ID_2));
        mapGgfsBlock(split3.file(), 0, 100, location(0, 100, ID_3));

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
    public void testNonGgfsOneBlockPerNode() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(false, "/file1", 0, 100, HOST_1);
        GridHadoopFileBlock split2 = split(false, "/file2", 0, 100, HOST_2);
        GridHadoopFileBlock split3 = split(false, "/file3", 0, 100, HOST_3);

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
    public void testGgfsSeveralBlocksPerNode() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1, HOST_2);
        GridHadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_1, HOST_2);
        GridHadoopFileBlock split3 = split(true, "/file3", 0, 100, HOST_1, HOST_3);

        mapGgfsBlock(split1.file(), 0, 100, location(0, 100, ID_1, ID_2));
        mapGgfsBlock(split2.file(), 0, 100, location(0, 100, ID_1, ID_2));
        mapGgfsBlock(split3.file(), 0, 100, location(0, 100, ID_1, ID_3));

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
    public void testNonGgfsSeveralBlocksPerNode() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(false, "/file1", 0, 100, HOST_1, HOST_2);
        GridHadoopFileBlock split2 = split(false, "/file2", 0, 100, HOST_1, HOST_2);
        GridHadoopFileBlock split3 = split(false, "/file3", 0, 100, HOST_1, HOST_3);

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
    public void testGgfsSeveralComplexBlocksPerNode() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(true, "/file1", 0, 100, HOST_1, HOST_2, HOST_3);
        GridHadoopFileBlock split2 = split(true, "/file2", 0, 100, HOST_1, HOST_2, HOST_3);

        mapGgfsBlock(split1.file(), 0, 100, location(0, 50, ID_1, ID_2), location(51, 100, ID_1, ID_3));
        mapGgfsBlock(split2.file(), 0, 100, location(0, 50, ID_1, ID_2), location(51, 100, ID_2, ID_3));

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
    public void testNonGgfsOrphans() throws IgniteCheckedException {
        GridHadoopFileBlock split1 = split(false, "/file1", 0, 100, INVALID_HOST_1, INVALID_HOST_2);
        GridHadoopFileBlock split2 = split(false, "/file2", 0, 100, INVALID_HOST_1, INVALID_HOST_3);
        GridHadoopFileBlock split3 = split(false, "/file3", 0, 100, INVALID_HOST_2, INVALID_HOST_3);

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
    private static GridHadoopMapReducePlan plan(int reducers, GridHadoopInputSplit... splits) throws IgniteCheckedException {
        assert reducers > 0;
        assert splits != null && splits.length > 0;

        Collection<GridHadoopInputSplit> splitList = new ArrayList<>(splits.length);

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

        GridHadoopMapReducePlan plan = PLANNER.preparePlan(new MockJob(reducers, splitList), top, null);

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
    private static boolean ensureMappers(UUID nodeId, GridHadoopInputSplit... expSplits) {
        Collection<GridHadoopInputSplit> expSplitsCol = new ArrayList<>();

        Collections.addAll(expSplitsCol, expSplits);

        Collection<GridHadoopInputSplit> splits = PLAN.get().mappers(nodeId);

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
     * @param ggfs GGFS flag.
     * @param file File.
     * @param start Start.
     * @param len Length.
     * @param hosts Hosts.
     * @return Split.
     */
    private static GridHadoopFileBlock split(boolean ggfs, String file, long start, long len, String... hosts) {
        URI uri = URI.create((ggfs ? "ggfs://ggfs@" : "hdfs://") + file);

        return new GridHadoopFileBlock(hosts, uri, start, len);
    }

    /**
     * Create block location.
     *
     * @param start Start.
     * @param len Length.
     * @param nodeIds Node IDs.
     * @return Block location.
     */
    private static IgniteFsBlockLocation location(long start, long len, UUID... nodeIds) {
        assert nodeIds != null && nodeIds.length > 0;

        Collection<ClusterNode> nodes = new ArrayList<>(nodeIds.length);

        for (UUID id : nodeIds)
            nodes.add(new GridTestNode(id));

        return new GridGgfsBlockLocationImpl(start, len, nodes);
    }

    /**
     * Map GGFS block to nodes.
     *
     * @param file File.
     * @param start Start.
     * @param len Length.
     * @param locations Locations.
     */
    private static void mapGgfsBlock(URI file, long start, long len, IgniteFsBlockLocation... locations) {
        assert locations != null && locations.length > 0;

        IgniteFsPath path = new IgniteFsPath(file);

        Block block = new Block(path, start, len);

        Collection<IgniteFsBlockLocation> locationsList = new ArrayList<>();

        Collections.addAll(locationsList, locations);

        BLOCK_MAP.put(block, locationsList);
    }

    /**
     * Block.
     */
    private static class Block {
        /** */
        private final IgniteFsPath path;

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
        private Block(IgniteFsPath path, long start, long len) {
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
     * Mocked job.
     */
    private static class MockJob implements GridHadoopJob {
        /** Reducers count. */
        private final int reducers;

        /** */
        private Collection<GridHadoopInputSplit> splitList;

        /**
         * Constructor.
         *
         * @param reducers Reducers count.
         * @param splitList Splits.
         */
        private MockJob(int reducers, Collection<GridHadoopInputSplit> splitList) {
            this.reducers = reducers;
            this.splitList = splitList;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobId id() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobInfo info() {
            return new GridHadoopDefaultJobInfo() {
                @Override public int reducers() {
                    return reducers;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public Collection<GridHadoopInputSplit> input() throws IgniteCheckedException {
            return splitList;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopTaskContext getTaskContext(GridHadoopTaskInfo info) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void initialize(boolean external, UUID nodeId) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void dispose(boolean external) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cleanupStagingDirectory() {
            // No-op.
        }
    }

    /**
     * Mocked GGFS.
     */
    private static class MockGgfs implements GridGgfsEx {
        /** {@inheritDoc} */
        @Override public boolean isProxy(URI path) {
            return PROXY_MAP.containsKey(path) && PROXY_MAP.get(path);
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len) {
            return BLOCK_MAP.get(new Block(path, start, len));
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len,
            long maxLen) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public GridGgfsContext context() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsPaths proxyPaths() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize, int seqReadsBeforePrefetch) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsStatus globalSpace() throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public Boolean globalSampling() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsLocalMetrics localMetrics() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long groupBlockSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> awaitDeletesAsync() throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String clientLogDirectory() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void clientLogDirectory(String logDir) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evictExclude(IgniteFsPath path, boolean primary) {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsConfiguration configuration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean exists(IgniteFsPath path) {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteFsFile info(IgniteFsPath path) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsPathSummary summary(IgniteFsPath path) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void rename(IgniteFsPath src, IgniteFsPath dest) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean delete(IgniteFsPath path, boolean recursive) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(IgniteFsPath path) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(IgniteFsPath path, @Nullable Map<String, String> props) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteFsPath> listPaths(IgniteFsPath path) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteFsFile> listFiles(IgniteFsPath path) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long usedSpaceSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> properties() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public IgniteFsOutputStream create(IgniteFsPath path, boolean overwrite) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite, int replication,
            long blockSize, @Nullable Map<String, String> props) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite,
            @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsOutputStream append(IgniteFsPath path, boolean create) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFsOutputStream append(IgniteFsPath path, int bufSize, boolean create,
            @Nullable Map<String, String> props) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setTimes(IgniteFsPath path, long accessTime, long modificationTime) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteFsMetrics metrics() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void resetMetrics() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long size(IgniteFsPath path) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void format() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
            Collection<IgniteFsPath> paths, @Nullable T arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
            Collection<IgniteFsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
            @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, @Nullable T arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
            @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, boolean skipNonExistentFiles,
            long maxRangeLen, @Nullable T arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid nextAffinityKey() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteFs withAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isAsync() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public <R> IgniteFuture<R> future() {
            return null;
        }
    }

    /**
     * Mocked Grid.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private static class MockIgnite extends IgniteSpringBean implements IgniteEx {
        /** {@inheritDoc} */
        @Override public IgniteFs ggfsx(String name) {
            assert F.eq("ggfs", name);

            return GGFS;
        }

        /** {@inheritDoc} */
        @Override public GridHadoop hadoop() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K extends GridCacheUtilityKey, V> GridCacheProjectionEx<K, V> utilityCache(Class<K> keyCls,
            Class<V> valCls) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <K, V> GridCache<K, V> cachex(@Nullable String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <K, V> GridCache<K, V> cachex() {
            return null;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Collection<GridCache<?, ?>> cachesx(@Nullable IgnitePredicate<? super GridCache<?, ?>>... p) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean eventUserRecordable(int type) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean allEventsUserRecordable(int[] types) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> compatibleVersions() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isJmxRemoteEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isRestartEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isSmtpEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<Boolean> sendAdminEmailAsync(String subj, String body, boolean html) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroupEx forSubjectId(UUID subjId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode localNode() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forLocal() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> ClusterNodeLocalMap<K, V> nodeLocalMap() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean pingNode(UUID nodeId) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public long topologyVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Collection<ClusterNode> topology(long topVer) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
            @Nullable Collection<? extends K> keys) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(File file,
            boolean restart, int timeout, int maxConn) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(
            Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
            int maxConn) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void stopNodes() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stopNodes(Collection<UUID> ids) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void restartNodes() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void restartNodes(Collection<UUID> ids) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void resetMetrics() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Ignite ignite() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forOthers(ClusterGroup prj) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forNodeIds(Collection<UUID> ids) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forNodeId(UUID id, UUID... ids) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forAttribute(String name, @Nullable String val) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forCacheNodes(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forDataNodes(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forClientNodes(String cacheName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forStreamer(String streamerName, @Nullable String... streamerNames) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forRemotes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forHost(ClusterNode node) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forDaemons() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forRandom() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forOldest() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forYoungest() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> nodes() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterNode node(UUID nid) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterNode node() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgnitePredicate<ClusterNode> predicate() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterMetrics metrics() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteCluster withAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isAsync() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public <R> IgniteFuture<R> future() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClusterGroup forCacheNodes(@Nullable String cacheName, Set<CacheDistributionMode> distributionModes) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String latestVersion() {
            return null;
        }
    }
}
