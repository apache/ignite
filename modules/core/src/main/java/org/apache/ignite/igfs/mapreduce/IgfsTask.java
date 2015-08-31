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

package org.apache.ignite.igfs.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS task which can be executed on the grid using one of {@code IgniteFs.execute()} methods. Essentially IGFS task
 * is regular {@link org.apache.ignite.compute.ComputeTask} with different map logic. Instead of implementing
 * {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method to split task into jobs, you must implement
 * {@link IgfsTask#createJob(org.apache.ignite.igfs.IgfsPath, IgfsFileRange, IgfsTaskArgs)} method.
 * <p>
 * Each file participating in IGFS task is split into {@link IgfsFileRange}s first. Normally range is a number of
 * consequent bytes located on a single node (see {@code IgfssGroupDataBlocksKeyMapper}). In case maximum range size
 * is provided (either through {@link org.apache.ignite.configuration.FileSystemConfiguration#getMaximumTaskRangeLength()} or {@code IgniteFs.execute()}
 * argument), then ranges could be further divided into smaller chunks.
 * <p>
 * Once file is split into ranges, each range is passed to {@code IgfsTask.createJob()} method in order to create a
 * {@link IgfsJob}.
 * <p>
 * Finally all generated jobs are sent to Grid nodes for execution.
 * <p>
 * As with regular {@code GridComputeTask} you can define your own logic for results handling and reduce step.
 * <p>
 * Here is an example of such a task:
 * <pre name="code" class="java">
 * public class WordCountTask extends IgfsTask&lt;String, Integer&gt; {
 *     &#64;Override
 *     public IgfsJob createJob(IgfsPath path, IgfsFileRange range, IgfsTaskArgs&lt;T&gt; args) throws IgniteCheckedException {
 *         // New job will be created for each range within each file.
 *         // We pass user-provided argument (which is essentially a word to look for) to that job.
 *         return new WordCountJob(args.userArgument());
 *     }
 *
 *     // Aggregate results into one compound result.
 *     public Integer reduce(List&lt;GridComputeJobResult&gt; results) throws IgniteCheckedException {
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
public abstract class IgfsTask<T, R> extends ComputeTaskAdapter<IgfsTaskArgs<T>, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Nullable @Override public final Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable IgfsTaskArgs<T> args) {
        assert ignite != null;
        assert args != null;

        IgniteFileSystem fs = ignite.fileSystem(args.igfsName());
        IgfsProcessorAdapter igfsProc = ((IgniteKernal) ignite).context().igfs();

        Map<ComputeJob, ClusterNode> splitMap = new HashMap<>();

        Map<UUID, ClusterNode> nodes = mapSubgrid(subgrid);

        for (IgfsPath path : args.paths()) {
            IgfsFile file = fs.info(path);

            if (file == null) {
                if (args.skipNonExistentFiles())
                    continue;
                else
                    throw new IgniteException("Failed to process IGFS file because it doesn't exist: " + path);
            }

            Collection<IgfsBlockLocation> aff = fs.affinity(path, 0, file.length(), args.maxRangeLength());

            long totalLen = 0;

            for (IgfsBlockLocation loc : aff) {
                ClusterNode node = null;

                for (UUID nodeId : loc.nodeIds()) {
                    node = nodes.get(nodeId);

                    if (node != null)
                        break;
                }

                if (node == null)
                    throw new IgniteException("Failed to find any of block affinity nodes in subgrid [loc=" + loc +
                        ", subgrid=" + subgrid + ']');

                IgfsJob job = createJob(path, new IgfsFileRange(file.path(), loc.start(), loc.length()), args);

                if (job != null) {
                    ComputeJob jobImpl = igfsProc.createJob(job, fs.name(), file.path(), loc.start(),
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
     * for IGFS file.
     *
     * @param path Path.
     * @param range File range based on consecutive blocks. This range will be further
     *      realigned to record boundaries on destination node.
     * @param args Task argument.
     * @return IGFS job. If {@code null} is returned, the passed in file range will be skipped.
     * @throws IgniteException If job creation failed.
     */
    @Nullable public abstract IgfsJob createJob(IgfsPath path, IgfsFileRange range,
        IgfsTaskArgs<T> args) throws IgniteException;

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