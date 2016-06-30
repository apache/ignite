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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Affinity serialization functions.
 */
public class PlatformAffinityFunctionSerializer {
    /**
     * Writes the affinity function context.
     * @param affCtx Affinity context.
     * @param writer Writer.
     * @param ctx Platform context.
     */
    public static void writeAffinityFunctionContext(AffinityFunctionContext affCtx, BinaryRawWriterEx writer,
        PlatformContext ctx) {
        List<List<ClusterNode>> prevAssignment = ((GridAffinityFunctionContextImpl)affCtx).prevAssignment();

        if (prevAssignment == null)
            writer.writeInt(-1);
        else {
            writer.writeInt(prevAssignment.size());

            for (List<ClusterNode> part : prevAssignment)
                ctx.writeNodes(writer, part);
        }

        // Write other props
        writer.writeInt(affCtx.backups());
        ctx.writeNodes(writer, affCtx.currentTopologySnapshot());
        writer.writeLong(affCtx.currentTopologyVersion().topologyVersion());
        writer.writeInt(affCtx.currentTopologyVersion().minorTopologyVersion());
        ctx.writeEvent(writer, affCtx.discoveryEvent());
    }

    /**
     * Reads the partition assignment.
     *
     * @param reader Reader.
     * @param ctx Platform context.
     * @return Partitions.
     */
    @NotNull
    public static List<List<ClusterNode>> readPartitionAssignment(BinaryRawReaderEx reader, PlatformContext ctx) {
        int partCnt = reader.readInt();
        List<List<ClusterNode>> res = new ArrayList<>(partCnt);
        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();

        for (int i = 0; i < partCnt; i++) {
            int partSize = reader.readInt();
            List<ClusterNode> part = new ArrayList<>(partSize);

            for (int j = 0; j < partSize; j++)
                part.add(cluster.node(reader.readUuid()));

            res.add(part);
        }

        return res;
    }
}
