/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.hadoop.planner.*;
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

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
    private static final MockGrid GRID = new MockGrid();

    /** Mocked GGFS. */
    private static final MockGgfs GGFS = new MockGgfs();

    /** Planner. */
    private static final GridHadoopDefaultMapReducePlanner PLANNER = new GridHadoopDefaultMapReducePlanner();

    /** Block locations. */
    private static final Map<Block, Collection<GridGgfsBlockLocation>> BLOCK_MAP = new HashMap<>();

    /** Proxy map. */
    private static final Map<URI, Boolean> PROXY_MAP = new HashMap<>();

    /** Last created plan. */
    private static final ThreadLocal<GridHadoopMapReducePlan> PLAN = new ThreadLocal<>();

    static {
        GridTestUtils.setFieldValue(PLANNER, "grid", GRID);
        GridTestUtils.setFieldValue(PLANNER, "log", new GridJavaLogger());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        BLOCK_MAP.clear();
        PROXY_MAP.clear();
    }

    /**
     * @throws GridException If failed.
     */
    public void testGgfsOneBlockPerNode() throws GridException {
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
     * @throws GridException If failed.
     */
    public void testNonGgfsOneBlockPerNode() throws GridException {
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
     * @throws GridException If failed.
     */
    public void testGgfsSeveralBlocksPerNode() throws GridException {
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
     * @throws GridException If failed.
     */
    public void testNonGgfsSeveralBlocksPerNode() throws GridException {
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
     * @throws GridException If failed.
     */
    public void testGgfsSeveralComplexBlocksPerNode() throws GridException {
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
     * @throws GridException If failed.
     */
    public void testNonGgfsOrphans() throws GridException {
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
     * @throws GridException If failed.
     */
    private static GridHadoopMapReducePlan plan(int reducers, GridHadoopInputSplit... splits) throws GridException {
        assert reducers > 0;
        assert splits != null && splits.length > 0;

        Collection<GridHadoopInputSplit> splitList = new ArrayList<>(splits.length);

        Collections.addAll(splitList, splits);

        Collection<GridNode> top = new ArrayList<>();

        GridTestNode node1 = new GridTestNode(ID_1);
        GridTestNode node2 = new GridTestNode(ID_2);
        GridTestNode node3 = new GridTestNode(ID_3);

        node1.setHostName(HOST_1);
        node2.setHostName(HOST_2);
        node3.setHostName(HOST_3);

        top.add(node1);
        top.add(node2);
        top.add(node3);

        GridHadoopMapReducePlan plan = PLANNER.preparePlan(splitList, top, new MockJob(reducers), null);

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
    private static GridGgfsBlockLocation location(long start, long len, UUID... nodeIds) {
        assert nodeIds != null && nodeIds.length > 0;

        Collection<GridNode> nodes = new ArrayList<>(nodeIds.length);

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
    private static void mapGgfsBlock(URI file, long start, long len, GridGgfsBlockLocation... locations) {
        assert locations != null && locations.length > 0;

        GridGgfsPath path = new GridGgfsPath(file);

        Block block = new Block(path, start, len);

        Collection<GridGgfsBlockLocation> locationsList = new ArrayList<>();

        Collections.addAll(locationsList, locations);

        BLOCK_MAP.put(block, locationsList);
    }

    /**
     * Mark URI as proxy.
     *
     * @param uri URI.
     */
    private static void proxy(URI uri) {
        PROXY_MAP.put(uri, true);
    }

    /**
     * Block.
     */
    private static class Block {
        /** */
        private final GridGgfsPath path;

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
        private Block(GridGgfsPath path, long start, long len) {
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
            int result = path.hashCode();

            result = 31 * result + (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (len ^ (len >>> 32));

            return result;
        }
    }

    /**
     * Mocked job.
     */
    private static class MockJob implements GridHadoopJob {
        /** Reducers count. */
        private final int reducers;

        /**
         * Constructor.
         *
         * @param reducers Reducers count.
         */
        private MockJob(int reducers) {
            this.reducers = reducers;
        }

        /** {@inheritDoc} */
        @Override public int reducers() {
            return reducers;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobId id() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobInfo info() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridHadoopInputSplit> input() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasCombiner() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopPartitioner partitioner() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization keySerialization() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopSerialization valueSerialization() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String property(String name) {
            return null;
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
        @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len)
            throws GridException {
            return BLOCK_MAP.get(new Block(path, start, len));
        }

        /** {@inheritDoc} */
        @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len,
            long maxLen) throws GridException {
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
        @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize, int seqReadsBeforePrefetch)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsStatus globalSpace() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void globalSampling(@Nullable Boolean val) throws GridException {
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
        @Override public GridFuture<?> awaitDeletesAsync() throws GridException {
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
        @Override public boolean evictExclude(GridGgfsPath path, boolean primary) {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsConfiguration configuration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean exists(GridGgfsPath path) throws GridException {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridGgfsFile info(GridGgfsPath path) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsPathSummary summary(GridGgfsPath path) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridGgfsFile update(GridGgfsPath path, Map<String, String> props)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void rename(GridGgfsPath src, GridGgfsPath dest) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean delete(GridGgfsPath path, boolean recursive) throws GridException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(GridGgfsPath path) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(GridGgfsPath path, @Nullable Map<String, String> props) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsOutputStream create(GridGgfsPath path, boolean overwrite) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsOutputStream create(GridGgfsPath path, int bufSize, boolean overwrite,
            @Nullable GridUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsOutputStream append(GridGgfsPath path, boolean create) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridGgfsOutputStream append(GridGgfsPath path, int bufSize, boolean create,
            @Nullable Map<String, String> props) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setTimes(GridGgfsPath path, long accessTime, long modificationTime) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public GridGgfsMetrics metrics() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void resetMetrics() throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long size(GridGgfsPath path) throws GridException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> format() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> GridFuture<R> execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
            Collection<GridGgfsPath> paths, @Nullable T arg) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> GridFuture<R> execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
            Collection<GridGgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> GridFuture<R> execute(Class<? extends GridGgfsTask<T, R>> taskCls,
            @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, @Nullable T arg)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T, R> GridFuture<R> execute(Class<? extends GridGgfsTask<T, R>> taskCls,
            @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, boolean skipNonExistentFiles,
            long maxRangeLen, @Nullable T arg) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridUuid nextAffinityKey() {
            return null;
        }
    }

    /**
     * Mocked Grid.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private static class MockGrid extends GridSpringBean {
        /** {@inheritDoc} */
        @Override public GridGgfs ggfs(String name) {
            assert F.eq("ggfs", name);

            return GGFS;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }
    }
}
