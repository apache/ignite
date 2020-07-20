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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.communication.tcp.AttributeNames;

/**
 * Common communication spi logic.
 */
public class CommunicationTcpUtils {
    /** No-op runnable. */
    public static final IgniteRunnable NOOP = () -> {};

    /**
     * @param node Node.
     * @return {@code True} if can use in/out connection pair for communication.
     */
    public static boolean usePairedConnections(ClusterNode node, String attributeName) {
        Boolean attr = node.attribute(attributeName);

        return attr != null && attr;
    }

    /**
     * Write message type to output stream.
     *
     * @param os Output stream.
     * @param type Message type.
     * @throws IOException On error.
     */
    public static void writeMessageType(OutputStream os, short type) throws IOException {
        os.write((byte)(type & 0xFF));
        os.write((byte)((type >> 8) & 0xFF));
    }

    /**
     * @param node Node.
     * @param filterReachableAddrs Filter addresses flag.
     * @return Node addresses.
     * @throws IgniteCheckedException If node does not have addresses.
     */
    public static Collection<InetSocketAddress> nodeAddresses(
        ClusterNode node,
        boolean filterReachableAddrs,
        AttributeNames attrs,
        Supplier<ClusterNode> localNode
    )
        throws IgniteCheckedException {
        Collection<String> rmtAddrs0 = node.attribute(attrs.addresses());
        Collection<String> rmtHostNames0 = node.attribute(attrs.hostNames());
        Integer boundPort = node.attribute(attrs.port());
        Collection<InetSocketAddress> extAddrs = node.attribute(attrs.externalizableAttributes());

        boolean isRmtAddrsExist = (!F.isEmpty(rmtAddrs0) && boundPort != null);
        boolean isExtAddrsExist = !F.isEmpty(extAddrs);

        if (!isRmtAddrsExist && !isExtAddrsExist)
            throw new IgniteCheckedException("Failed to send message to the destination node. Node doesn't have any " +
                "TCP communication addresses or mapped external addresses. Check configuration and make sure " +
                "that you use the same communication SPI on all nodes. Remote node id: " + node.id());

        LinkedHashSet<InetSocketAddress> addrs;

        // Try to connect first on bound addresses.
        if (isRmtAddrsExist) {
            List<InetSocketAddress> addrs0 = new ArrayList<>(U.toSocketAddresses(rmtAddrs0, rmtHostNames0, boundPort, true));

            boolean sameHost = U.sameMacs(localNode.get(), node);

            addrs0.sort(U.inetAddressesComparator(sameHost));

            addrs = new LinkedHashSet<>(addrs0);
        }
        else
            addrs = new LinkedHashSet<>();

        // Then on mapped external addresses.
        if (isExtAddrsExist)
            addrs.addAll(extAddrs);

        if (filterReachableAddrs) {
            Set<InetAddress> allInetAddrs = U.newHashSet(addrs.size());

            for (InetSocketAddress addr : addrs) {
                // Skip unresolved as addr.getAddress() can return null.
                if (!addr.isUnresolved())
                    allInetAddrs.add(addr.getAddress());
            }

            List<InetAddress> reachableInetAddrs = U.filterReachable(allInetAddrs);

            if (reachableInetAddrs.size() < allInetAddrs.size()) {
                LinkedHashSet<InetSocketAddress> addrs0 = U.newLinkedHashSet(addrs.size());

                List<InetSocketAddress> unreachableInetAddr = new ArrayList<>(allInetAddrs.size() - reachableInetAddrs.size());

                for (InetSocketAddress addr : addrs) {
                    if (reachableInetAddrs.contains(addr.getAddress()))
                        addrs0.add(addr);
                    else
                        unreachableInetAddr.add(addr);
                }

                addrs0.addAll(unreachableInetAddr);

                addrs = addrs0;
            }
        }

        return addrs;
    }

    /**
     * Returns handshake exception with specific message.
     */
    public static IgniteSpiOperationTimeoutException handshakeTimeoutException() {
        return new IgniteSpiOperationTimeoutException("Failed to perform handshake due to timeout " +
            "(consider increasing 'connectionTimeout' configuration property).");
    }

    /**
     * @param errs Error.
     * @return {@code True} if error was caused by some connection IO error or IgniteCheckedException due to timeout.
     */
    public static boolean isRecoverableException(Exception errs) {
        return X.hasCause(
            errs,
            IOException.class,
            HandshakeException.class,
            IgniteSpiOperationTimeoutException.class
        );
    }
}
