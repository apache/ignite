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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.internal.IgniteComputeImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManager;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Compute instance. */
    private final IgniteComputeImpl compute;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param compute Target compute instance.
     * @param taskPtr Pointer to the task in the native platform.
     * @param topVer Initial topology version.
     */
    public PlatformFullTask(PlatformContext ctx, IgniteComputeImpl compute, long taskPtr, long topVer) {
        super(ctx, taskPtr);

        this.compute = compute;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert arg == null;

        lock.readLock().lock();

        try {
            assert !done;

            Collection<ClusterNode> nodes = compute.clusterGroup().nodes();

            PlatformMemoryManager memMgr = ctx.memory();

            try (PlatformMemory mem = memMgr.allocate()) {
                PlatformOutputStream out = mem.output();

                BinaryRawWriterEx writer = ctx.writer(out);

                writer.writeLong(taskPtr);

                write(writer, nodes, subgrid);

                out.synchronize();

                ctx.gateway().computeTaskMap(mem.pointer());

                PlatformInputStream in = mem.input();

                in.synchronize();

                BinaryRawReaderEx reader = ctx.reader(in);

                return read(reader, nodes);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Write topology information.
     *
     * @param writer Writer.
     * @param nodes Current topology nodes.
     * @param subgrid Subgrid.
     */
    private void write(BinaryRawWriterEx writer, Collection<ClusterNode> nodes, List<ClusterNode> subgrid) {
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
    private Map<ComputeJob, ClusterNode> read(BinaryRawReaderEx reader, Collection<ClusterNode> nodes) {
        if (reader.readBoolean()) {
            if (!reader.readBoolean())
                return null;

            int size = reader.readInt();

            Map<ComputeJob, ClusterNode> map = U.newHashMap(size);

            for (int i = 0; i < size; i++) {
                long ptr = reader.readLong();

                Object nativeJob = reader.readBoolean() ? reader.readObjectDetached() : null;

                PlatformJob job = ctx.createJob(this, ptr, nativeJob);

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
