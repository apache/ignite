/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.planner;

import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * TODO-gg-8170 Change implementation.
 * Simple GGFS-aware map-reduce planner.
 */
public class GridHadoopDefaultMapReducePlanner implements GridHadoopMapReducePlanner {
    /** Injected grid. */
    @GridInstanceResource
    private Grid grid;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public GridHadoopMapReducePlan preparePlan(Collection<GridHadoopInputSplit> splits,
        Collection<GridNode> top, GridHadoopJob job, @Nullable GridHadoopMapReducePlan oldPlan) throws GridException {

        Map<UUID, Collection<GridHadoopInputSplit>> mappersMap = mappersMap(job, splits, top);

        return new GridHadoopDefaultMapReducePlan(mappersMap, reducersMap(top, job));
    }

    /**
     * Gets mappers to nodes assignment.
     *
     * @param job Job.
     * @param splits Splits to assign to nodes.
     * @param top Topology.
     * @return Block to node assignment.
     */
    private Map<UUID, Collection<GridHadoopInputSplit>> mappersMap(GridHadoopJob job,
        Iterable<GridHadoopInputSplit> splits, Collection<GridNode> top) throws GridException {
        Map<UUID, Collection<GridHadoopInputSplit>> mappersMap = new HashMap<>();

        Map<String, Collection<GridNode>> nodes = groupByHost(top);

        for (GridHadoopInputSplit split : splits) {
            UUID nodeId = nodeId(job, split, top, nodes);

            if (log.isDebugEnabled())
                log.debug("Mapped split to node [split=" + split + ", nodeId=" + nodeId + ']');

            Collection<GridHadoopInputSplit> nodeSplits = mappersMap.get(nodeId);

            if (nodeSplits == null) {
                nodeSplits = new ArrayList<>();

                mappersMap.put(nodeId, nodeSplits);
            }

            nodeSplits.add(split);
        }
        return mappersMap;
    }

    /**
     * Groups nodes by host names.
     *
     * @param top Topology to group.
     * @return Map.
     */
    private Map<String, Collection<GridNode>> groupByHost(Collection<GridNode> top) {
        Map<String, Collection<GridNode>> grouped = new HashMap<>(top.size());

        for (GridNode node : top) {
            for (String host : node.hostNames()) {
                Collection<GridNode> nodes = grouped.get(host);

                if (nodes == null) {
                    // Expecting 1-2 nodes per host.
                    nodes = new ArrayList<>(2);

                    grouped.put(host, nodes);
                }

                nodes.add(node);
            }
        }

        return grouped;
    }

    /**
     * Gets node ID for given file split.
     *
     * @param job Job.
     * @param split Block.
     * @param top Topology.
     * @throws GridException If failed to assign splits to nodes.
     * @return Node ID.
     */
    private UUID nodeId(GridHadoopJob job, GridHadoopInputSplit split, Collection<GridNode> top,
        Map<String, Collection<GridNode>> hostNodeMap) throws GridException {

        GridHadoopFileBlock block = (GridHadoopFileBlock) split;

        if (log.isDebugEnabled())
            log.debug("Mapping split to node: " + split);

        // TODO-gg-8170 change to getFileBlockLocations.
        GridGgfs ggfs = ggfsForBlock(job, block);

        if (ggfs != null) {
            if (log.isDebugEnabled())
                log.debug("Mapping file block according to ggfs affinity: " + block);

            Collection<GridGgfsBlockLocation> aff = ggfs.affinity(new GridGgfsPath(block.file()), block.start(),
                block.length());

            // TODO-gg-8170 do we need to handle collection or we can assert aff.size() == 1?
            long maxLen = Long.MIN_VALUE;
            UUID max = null;

            for (GridGgfsBlockLocation loc : aff) {
                if (maxLen < loc.length()) {
                    maxLen = loc.length();
                    max = F.first(loc.nodeIds());
                }
            }

            return max;
        }
        else {
            // Try to select node according to hostname.
            for (String host : split.hosts()) {
                GridNode node = F.first(hostNodeMap.get(host));

                if (node != null)
                    return node.id();
            }

            if (log.isDebugEnabled())
                log.debug("Could not find node by hostname, returning random node: " + split);

            // No nodes were selected based on host, return random node.
            int idx = ThreadLocalRandom8.current().nextInt(top.size());

            Iterator<GridNode> it = top.iterator();

            int i = 0;

            while (it.hasNext()) {
                GridNode node = it.next();

                if (i == idx)
                    return node.id();

                i++;
            }

            return null;
        }
    }

    /**
     * Gets GGFS instance for given block, if such instance exists.
     *
     * @param job Job.
     * @param block Block.
     * @return GGFS instance, if available.
     */
    @Nullable private GridGgfs ggfsForBlock(GridHadoopJob job, GridHadoopFileBlock block) {
        try {
            Path path = new Path(block.file());

            FileSystem fs = path.getFileSystem(((GridHadoopDefaultJobInfo)job.info()).configuration());

            if (log.isDebugEnabled())
                log.debug("Got file system for path [fs=" + fs + ", path=" + path + ']');

            if (fs instanceof GridGgfsHadoopFileSystem)
                return grid.ggfs("ggfs"); // TODO-gg-8170 change to getFileBlockLocations.

            return null;
        }
        catch (IOException ignored) {
            return null;
        }
    }

    /**
     * Evenly distributes reducers to topology nodes.
     *
     * @param top Topology.
     * @param job Job.
     * @return Reducers map.
     */
    private Map<UUID, int[]> reducersMap(Collection<GridNode> top, GridHadoopJob job) {
        Map<UUID, int[]> rdcMap = new HashMap<>(top.size());

        int avgCnt = job.reducers() / top.size();
        int lastCnt = job.reducers() - avgCnt * (top.size() - 1);

        assert lastCnt >= 0 : lastCnt;

        int i = 0;

        for (GridNode n : top) {
            int cnt = i == (top.size() - 1) ? lastCnt : avgCnt;

            int[] rdc = new int[cnt];

            for (int r = 0; r < cnt; r++)
                rdc[r] = i * avgCnt + r;

            rdcMap.put(n.id(), rdc);

            i++;
        }

        return rdcMap;
    }
}
