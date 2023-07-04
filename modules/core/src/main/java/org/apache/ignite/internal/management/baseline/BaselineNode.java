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

package org.apache.ignite.internal.management.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for {@link org.apache.ignite.cluster.BaselineNode}.
 */
public class BaselineNode extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String consistentId;

    /** */
    private Map<String, Object> attrs;

    /** */
    private @Nullable Long order;

    /**
     * Resolved list of (ip, hostname) pairs
     * (if ip has no resolved host, hostname will be the string representation of ip).
     */
    private @NotNull Collection<ResolvedAddresses> addrs = Collections.emptyList();

    /**
     * Default constructor.
     */
    public BaselineNode() {
        // No-op.
    }

    /**
     * Create data transfer object for baseline node.
     *
     * @param node Baseline node.
     * @param resolvedInetAddrs List of resolved ip, hostnames pairs.
     */
    public BaselineNode(org.apache.ignite.cluster.BaselineNode node, @NotNull Collection<ResolvedAddresses> resolvedInetAddrs) {
        consistentId = String.valueOf(node.consistentId());
        attrs = node.attributes();

        //Baseline topology returns instances of DetachedClusternode
        if (node instanceof IgniteClusterNode) {
            order = ((IgniteClusterNode)node).order();
            addrs = resolvedInetAddrs;
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /**
     * @return Node consistent ID.
     */
    public String getConsistentId() {
        return consistentId;
    }

    /**
     * @return Node attributes.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /**
     * @return Node order.
     */
    public @Nullable Long getOrder() {
        return order;
    }

    /**
     *
     * @return Collection with resolved pairs ip->hostname
     */
    public @NotNull Collection<ResolvedAddresses> getAddrs() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, consistentId);
        U.writeMap(out, attrs);
        out.writeObject(order);
        U.writeCollection(out, addrs);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        consistentId = U.readString(in);
        attrs = U.readMap(in);

        if (protoVer >= V2)
            order = (Long)in.readObject();

        if (protoVer >= V3) {
            Collection<ResolvedAddresses> inputAddrs = U.readCollection(in);

            if (inputAddrs != null)
                addrs = inputAddrs;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineNode.class, this);
    }

    /**
     * Simple data class for storing (hostname, address) pairs
     */
    public static class ResolvedAddresses extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String hostname;

        /** Textual representation of IP address. */
        private String addr;

        /**
         * @param inetAddr Inet address.
         */
        ResolvedAddresses(InetAddress inetAddr) {
            this.hostname = inetAddr.getHostName();
            this.addr = inetAddr.getHostAddress();
        }

        /**
         * Default constructor.
         */
        public ResolvedAddresses() {
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeString(out, hostname);
            U.writeString(out, addr);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in)
            throws IOException, ClassNotFoundException {
            hostname = U.readString(in);
            addr = U.readString(in);
        }

        /**
         * @return Hostname.
         */
        public String hostname() {
            return hostname;
        }

        /**
         * @return Textual representation of IP address.
         */
        public String address() {
            return addr;
        }
    }
}
