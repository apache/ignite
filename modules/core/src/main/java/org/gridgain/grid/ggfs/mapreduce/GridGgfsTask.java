/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * GGFS task which can be executed on the grid using one of {@code GridGgfs.execute()} methods. Essentially GGFS task
 * is regular {@link org.apache.ignite.compute.ComputeTask} with different map logic. Instead of implementing
 * {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method to split task into jobs, you must implement
 * {@link GridGgfsTask#createJob(org.gridgain.grid.ggfs.IgniteFsPath, GridGgfsFileRange, GridGgfsTaskArgs)} method.
 * <p>
 * Each file participating in GGFS task is split into {@link GridGgfsFileRange}s first. Normally range is a number of
 * consequent bytes located on a single node (see {@code GridGgfsGroupDataBlocksKeyMapper}). In case maximum range size
 * is provided (either through {@link org.gridgain.grid.ggfs.IgniteFsConfiguration#getMaximumTaskRangeLength()} or {@code GridGgfs.execute()}
 * argument), then ranges could be further divided into smaller chunks.
 * <p>
 * Once file is split into ranges, each range is passed to {@code GridGgfsTask.createJob()} method in order to create a
 * {@link GridGgfsJob}.
 * <p>
 * Finally all generated jobs are sent to Grid nodes for execution.
 * <p>
 * As with regular {@code GridComputeTask} you can define your own logic for results handling and reduce step.
 * <p>
 * Here is an example of such a task:
 * <pre name="code" class="java">
 * public class WordCountTask extends GridGgfsTask&lt;String, Integer&gt; {
 *     &#64;Override
 *     public GridGgfsJob createJob(GridGgfsPath path, GridGgfsFileRange range, GridGgfsTaskArgs&lt;T&gt; args) throws GridException {
 *         // New job will be created for each range within each file.
 *         // We pass user-provided argument (which is essentially a word to look for) to that job.
 *         return new WordCountJob(args.userArgument());
 *     }
 *
 *     // Aggregate results into one compound result.
 *     public Integer reduce(List&lt;GridComputeJobResult&gt; results) throws GridException {
 *         Integer total = 0;
 *
 *         for (GridComputeJobResult res : results) {
 *             Integer cnt = res.getData();
 *
 *             // Null can be returned for non-existent file in case we decide to ignore such situations.
 *             if (cnt != null)
 *                 total += cnt;
 *         }
 *
 *         return total;
 *     }
 * }
 * </pre>
 */
public abstract class GridGgfsTask<T, R> extends ComputeTaskAdapter<GridGgfsTaskArgs<T>, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Nullable @Override public final Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable GridGgfsTaskArgs<T> args) throws GridException {
        assert ignite != null;
        assert args != null;

        IgniteFs ggfs = ignite.fileSystem(args.ggfsName());
        GridGgfsProcessorAdapter ggfsProc = ((GridKernal) ignite).context().ggfs();

        Map<ComputeJob, ClusterNode> splitMap = new HashMap<>();

        Map<UUID, ClusterNode> nodes = mapSubgrid(subgrid);

        for (IgniteFsPath path : args.paths()) {
            IgniteFsFile file = ggfs.info(path);

            if (file == null) {
                if (args.skipNonExistentFiles())
                    continue;
                else
                    throw new GridException("Failed to process GGFS file because it doesn't exist: " + path);
            }

            Collection<IgniteFsBlockLocation> aff = ggfs.affinity(path, 0, file.length(), args.maxRangeLength());

            long totalLen = 0;

            for (IgniteFsBlockLocation loc : aff) {
                ClusterNode node = null;

                for (UUID nodeId : loc.nodeIds()) {
                    node = nodes.get(nodeId);

                    if (node != null)
                        break;
                }

                if (node == null)
                    throw new GridException("Failed to find any of block affinity nodes in subgrid [loc=" + loc +
                        ", subgrid=" + subgrid + ']');

                GridGgfsJob job = createJob(path, new GridGgfsFileRange(file.path(), loc.start(), loc.length()), args);

                if (job != null) {
                    ComputeJob jobImpl = ggfsProc.createJob(job, ggfs.name(), file.path(), loc.start(),
                        loc.length(), args.recordResolver());

                    splitMap.put(jobImpl, node);
                }

                totalLen += loc.length();
            }

            assert totalLen == file.length();
        }

        return splitMap;
    }

    /**
     * Callback invoked during task map procedure to create job that will process specified split
     * for GGFS file.
     *
     * @param path Path.
     * @param range File range based on consecutive blocks. This range will be further
     *      realigned to record boundaries on destination node.
     * @param args Task argument.
     * @return GGFS job. If {@code null} is returned, the passed in file range will be skipped.
     * @throws GridException If job creation failed.
     */
    @Nullable public abstract GridGgfsJob createJob(IgniteFsPath path, GridGgfsFileRange range,
        GridGgfsTaskArgs<T> args) throws GridException;

    /**
     * Maps list by node ID.
     *
     * @param subgrid Subgrid.
     * @return Map.
     */
    private Map<UUID, ClusterNode> mapSubgrid(Collection<ClusterNode> subgrid) {
        Map<UUID, ClusterNode> res = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            res.put(node.id(), node);

        return res;
    }
}
