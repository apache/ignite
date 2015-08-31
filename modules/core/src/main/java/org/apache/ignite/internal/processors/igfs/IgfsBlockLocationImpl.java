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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * File block location in the grid.
 */
public class IgfsBlockLocationImpl implements IgfsBlockLocation, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long start;

    /** */
    private long len;

    /** */
    @GridToStringInclude
    private Collection<UUID> nodeIds;

    /** */
    private Collection<String> names;

    /** */
    private Collection<String> hosts;

    /**
     * Empty constructor for externalizable.
     */
    public IgfsBlockLocationImpl() {
        // No-op.
    }

    /**
     * @param location HDFS block location.
     * @param len New length.
     */
    public IgfsBlockLocationImpl(IgfsBlockLocation location, long len) {
        assert location != null;

        start = location.start();
        this.len = len;

        nodeIds = location.nodeIds();
        names = location.names();
        hosts = location.hosts();
    }

    /**
     * @param start Start.
     * @param len Length.
     * @param nodes Affinity nodes.
     */
    public IgfsBlockLocationImpl(long start, long len, Collection<ClusterNode> nodes) {
        assert start >= 0;
        assert len > 0;
        assert nodes != null && !nodes.isEmpty();

        this.start = start;
        this.len = len;

        convertFromNodes(nodes);
    }

    /**
     * @return Start position.
     */
    @Override public long start() {
        return start;
    }

    /**
     * @return Length.
     */
    @Override public long length() {
        return len;
    }

    /**
     * @return Node IDs.
     */
    @Override public Collection<UUID> nodeIds() {
        return nodeIds;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> names() {
        return names;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hosts() {
        return hosts;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int)(start ^ (start >>> 32));

        res = 31 * res + (int)(len ^ (len >>> 32));
        res = 31 * res + nodeIds.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IgfsBlockLocationImpl that = (IgfsBlockLocationImpl)o;

        return len == that.len && start == that.start && F.eq(nodeIds, that.nodeIds) && F.eq(names, that.names) &&
            F.eq(hosts, that.hosts);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsBlockLocationImpl.class, this);
    }

    /**
     * Writes this object to data output. Note that this is not externalizable
     * interface because we want to eliminate any marshaller.
     *
     * @param out Data output to write.
     * @throws IOException If write failed.
     */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert names != null;
        assert hosts != null;

        out.writeLong(start);
        out.writeLong(len);

        out.writeBoolean(nodeIds != null);

        if (nodeIds != null) {
            out.writeInt(nodeIds.size());

            for (UUID nodeId : nodeIds)
                U.writeUuid(out, nodeId);
        }

        out.writeInt(names.size());

        for (String name : names)
            out.writeUTF(name);

        out.writeInt(hosts.size());

        for (String host : hosts)
            out.writeUTF(host);
    }

    /**
     * Reads object from data input. Note we do not use externalizable interface
     * to eliminate marshaller.
     *
     * @param in Data input.
     * @throws IOException If read failed.
     */
    @Override public void readExternal(ObjectInput in) throws IOException {
        start = in.readLong();
        len = in.readLong();

        int size;

        if (in.readBoolean()) {
            size = in.readInt();

            nodeIds = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                nodeIds.add(U.readUuid(in));
        }

        size = in.readInt();

        names = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            names.add(in.readUTF());

        size = in.readInt();

        hosts = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            hosts.add(in.readUTF());
    }

    /**
     * Converts collection of rich nodes to block location data.
     *
     * @param nodes Collection of affinity nodes.
     */
    private void convertFromNodes(Collection<ClusterNode> nodes) {
        Collection<String> names = new LinkedHashSet<>();
        Collection<String> hosts = new LinkedHashSet<>();
        Collection<UUID> nodeIds = new ArrayList<>(nodes.size());

        for (final ClusterNode node : nodes) {
            // Normalize host names into Hadoop-expected format.
            try {
                Collection<InetAddress> addrs = U.toInetAddresses(node);

                for (InetAddress addr : addrs) {
                    if (addr.getHostName() == null)
                        names.add(addr.getHostAddress() + ":" + 9001);
                    else {
                        names.add(addr.getHostName() + ":" + 9001); // hostname:portNumber
                        hosts.add(addr.getHostName());
                    }
                }
            }
            catch (IgniteCheckedException ignored) {
                names.addAll(node.addresses());
            }

            nodeIds.add(node.id());
        }

        this.nodeIds = nodeIds;
        this.names = names;
        this.hosts = hosts;
    }
}