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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
        assert affCtx != null;
        assert writer != null;
        assert ctx != null;

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
     * Writes the affinity function context.
     * @param reader Reader.
     * @param ctx Platform context.
     */
    public static AffinityFunctionContext readAffinityFunctionContext(BinaryRawReader reader,
        PlatformContext ctx) {
        assert reader != null;
        assert ctx != null;

        List<ClusterNode> topSnapshot = readNodes(reader, ctx);

        List<List<ClusterNode>> prevAssignment = null;

        int partCnt = reader.readInt();

        if (partCnt > 0) {
            prevAssignment = new ArrayList<>(partCnt);

            for (int i = 0; i < partCnt; i++)
                prevAssignment.add(readNodes(reader, ctx));
        }

        // NOTE: this event won't be entirely valid, since new id and timestamp will be generated.
        // This is not an issue with current Affinity implementations,
        // and platform only allows overriding predefined implementations.
        DiscoveryEvent discoEvt = new DiscoveryEvent(readNode(reader, ctx), reader.readString(), reader.readInt(),
            readNode(reader, ctx));

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(reader.readLong(), reader.readInt());

        int backups = reader.readInt();

        return new GridAffinityFunctionContextImpl(topSnapshot, prevAssignment, discoEvt, topVer, backups);
    }

    /**
     * Reads nodes from a stream.
     *
     * @param reader Reader.
     * @param ctx Platform context.
     * @return Node list.
     */
    private static List<ClusterNode> readNodes(BinaryRawReader reader, PlatformContext ctx) {
        assert reader != null;
        assert ctx != null;

        int nodeCnt = reader.readInt();
        List<ClusterNode> nodes = new ArrayList<>(nodeCnt);

        for (int i = 0; i < nodeCnt; i++)
            nodes.add(readNode(reader, ctx));

        return nodes;
    }

    /**
     * Reads node from a stream.
     *
     * @param reader Reader.
     * @param ctx Platform context.
     * @return Node.
     */
    private static ClusterNode readNode(BinaryRawReader reader, PlatformContext ctx) {
        return ctx.kernalContext().grid().cluster().node(reader.readUuid());
    }

    /**
     * Writes the partition assignment to a stream.
     *
     * @param partitions Partitions.
     * @param writer Writer.
     */
    public static void writePartitionAssignment(List<List<ClusterNode>> partitions, BinaryRawWriterEx writer,
        PlatformContext ctx) {
        assert partitions != null;
        assert writer != null;

        writer.writeInt(partitions.size());

        for (List<ClusterNode> part : partitions)
            ctx.writeNodes(writer, part);
    }

    /**
     * Reads the partition assignment.
     *
     * @param reader Reader.
     * @param ctx Platform context.
     * @return Partitions.
     */
    @NotNull
    public static List<List<ClusterNode>> readPartitionAssignment(BinaryRawReader reader, PlatformContext ctx) {
        assert reader != null;
        assert ctx != null;

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
