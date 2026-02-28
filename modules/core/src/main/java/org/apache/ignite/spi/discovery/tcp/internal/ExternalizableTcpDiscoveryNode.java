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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/** A {@link TcpDiscoveryNode} supporting {@link Externalizable}. */
public class ExternalizableTcpDiscoveryNode extends TcpDiscoveryNode implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Empty ctor. to support {@link Externalizable}. */
    public ExternalizableTcpDiscoveryNode() {
        // No-op.
    }

    /** @param clusterNode Tcp discovery node impl. */
    private ExternalizableTcpDiscoveryNode(ClusterNode clusterNode) {
        id = clusterNode.id();
        addrs = clusterNode.addresses();
        hostNames = clusterNode.hostNames();

        metrics = clusterNode.metrics();
        order = clusterNode.order();
        ver = clusterNode.version();

        loc = clusterNode.isLocal();

        if (clusterNode instanceof TcpDiscoveryNode) {
            TcpDiscoveryNode node = (TcpDiscoveryNode)clusterNode;

            discPort = node.discoveryPort();
            attrs = node.getAttributes();
            intOrder = node.internalOrder();
            clientRouterNodeId = node.clientRouterNodeId();
        }
        else
            attrs = clusterNode.attributes();

        consistentId = clusterNode.consistentId();
    }

    /** @return {@link Externalizable} version of {@link ClusterNode}. */
    public static @Nullable ClusterNode of(@Nullable ClusterNode clusterNode) {
        return clusterNode instanceof Externalizable
            ? clusterNode
            : clusterNode == null ? null : new ExternalizableTcpDiscoveryNode(clusterNode);
    }

    /** @return {@link Externalizable} collection of {@link ClusterNode}s. */
    public static ArrayList<ClusterNode> of(Collection<? extends ClusterNode> nodes) {
        return nodes.stream().map(ExternalizableTcpDiscoveryNode::of).collect(Collectors.toCollection(ArrayList::new));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, id());
        U.writeMap(out, getAttributes());
        U.writeCollection(out, addresses());
        U.writeCollection(out, hostNames());
        out.writeInt(discoveryPort());

        // Cluster metrics
        byte[] mtr = null;

        ClusterMetrics metrics = metrics();

        if (metrics != null)
            mtr = ClusterMetricsSnapshot.serialize(metrics);

        U.writeByteArray(out, mtr);

        // Legacy: Number of cache metrics
        out.writeInt(0);

        out.writeLong(order());
        out.writeLong(internalOrder());
        out.writeObject(version());
        U.writeUuid(out, clientRouterNodeId());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readUuid(in);

        attrs = U.readMap(in);
        addrs = U.readCollection(in);
        hostNames = U.readCollection(in);
        discPort = in.readInt();

        Object consistentIdAttr = getAttributes().get(ATTR_NODE_CONSISTENT_ID);

        // Cluster metrics
        byte[] mtr = U.readByteArray(in);

        if (mtr != null)
            metrics = ClusterMetricsSnapshot.deserialize(mtr, 0);

        // Legacy: Cache metrics
        int size = in.readInt();
        assert size == 0;

        order = in.readLong();
        intOrder = in.readLong();
        ver = (IgniteProductVersion)in.readObject();
        clientRouterNodeId = U.readUuid(in);

        if (clientRouterNodeId() != null)
            consistentId = consistentIdAttr != null ? consistentIdAttr : id;
        else
            consistentId = consistentIdAttr != null ? consistentIdAttr : U.consistentId(addrs, discPort);
    }
}
