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

package org.apache.ignite.internal.processors.platform.compute;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxy;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxyImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManager;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.TaskSessionResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interop task which requires full execution cycle.
 */
@ComputeTaskNoResultCache
public final class PlatformFullTask extends PlatformAbstractTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial topology version. */
    private final long topVer;

    /** Cluster group. */
    private final ClusterGroup grp;

    /** Platform task name. */
    private final String taskName;

    /** {@code true} if distribution of the session attributes should be enabled. */
    private final boolean taskSesFullSupport;

    /** The task session. */
    @TaskSessionResource
    private ComputeTaskSession ses;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param grp Cluster group.
     * @param taskPtr Pointer to the task in the native platform.
     * @param topVer Initial topology version.
     * @param taskName Task name.
     * @param taskSesFullSupport {@code true} if distribution of the session attributes should be enabled.
     */
    public PlatformFullTask(
        PlatformContext ctx,
        ClusterGroup grp,
        long taskPtr,
        long topVer,
        String taskName,
        boolean taskSesFullSupport) {
        super(ctx, taskPtr);

        this.grp = grp;
        this.topVer = topVer;
        this.taskName = taskName;
        this.taskSesFullSupport = taskSesFullSupport;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert arg == null;

        lock.readLock().lock();

        try {
            assert !done;

            Collection<ClusterNode> nodes = grp.nodes();

            PlatformMemoryManager memMgr = ctx.memory();

            final PlatformTarget platformSes = new PlatformComputeTaskSession(ctx, ses);
            final PlatformTargetProxy platformSesProxy = new PlatformTargetProxyImpl(platformSes, ctx);

            try (PlatformMemory mem = memMgr.allocate()) {
                PlatformOutputStream out = mem.output();

                BinaryWriterEx writer = ctx.writer(out);

                writer.writeLong(taskPtr);

                write(writer, nodes, subgrid);

                out.synchronize();

                ctx.gateway().computeTaskMap(mem.pointer(), platformSesProxy);

                PlatformInputStream in = mem.input();

                in.synchronize();

                BinaryReaderEx reader = ctx.reader(in);

                return read(reader, nodes);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@code true} if distribution of session attributes should be enabled. */
    public boolean taskSessionFullSupport() {
        return taskSesFullSupport;
    }

    /**
     * Write topology information.
     *
     * @param writer Writer.
     * @param nodes Current topology nodes.
     * @param subgrid Subgrid.
     */
    private void write(BinaryWriterEx writer, Collection<ClusterNode> nodes, List<ClusterNode> subgrid) {
        GridDiscoveryManager discoMgr = ctx.kernalContext().discovery();

        long curTopVer = discoMgr.topologyVersion();

        if (topVer != curTopVer) {
            writer.writeBoolean(true);

            writer.writeLong(curTopVer);

            writer.writeInt(nodes.size());

            // Write subgrid size for more precise collection allocation on native side.
            writer.writeInt(subgrid.size());

            for (ClusterNode node : nodes) {
                ctx.writeNode(writer, node);
                writer.writeBoolean(subgrid.contains(node));
            }
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Read map result.
     *
     * @param reader Reader.
     * @param nodes Current topology nodes.
     * @return Map result.
     */
    private Map<ComputeJob, ClusterNode> read(BinaryReaderEx reader, Collection<ClusterNode> nodes) {
        if (reader.readBoolean()) {
            if (!reader.readBoolean())
                return null;

            int size = reader.readInt();

            Map<ComputeJob, ClusterNode> map = U.newHashMap(size);

            for (int i = 0; i < size; i++) {
                long ptr = reader.readLong();

                Object nativeJob = reader.readBoolean() ? reader.readObjectDetached() : null;

                PlatformJob job = ctx.createJob(this, ptr, nativeJob, taskName);

                UUID jobNodeId = reader.readUuid();

                assert jobNodeId != null;

                ClusterNode jobNode = ctx.kernalContext().discovery().node(jobNodeId);

                if (jobNode == null) {
                    // Special case when node has left the grid at this point.
                    // We expect task processor to perform necessary failover.
                    for (ClusterNode node : nodes) {
                        if (node.id().equals(jobNodeId)) {
                            jobNode = node;

                            break;
                        }
                    }

                    assert jobNode != null;
                }

                map.put(job, jobNode);
            }

            return map;
        }
        else
            throw new IgniteException(reader.readString());
    }
}
