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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Nodes discovery context for Ignite client.
 */
public class ClientDiscoveryContext {
    /** Indicates unknown topology version. */
    private static final long UNKNOWN_TOP_VER = -1;

    /** */
    private final AtomicBoolean refreshIsInProgress = new AtomicBoolean();

    /** */
    private final IgniteLogger log;

    /** Statically configured addresses. */
    @Nullable private final String[] addresses;

    /** Configured address finder. */
    @Nullable private final ClientAddressFinder addrFinder;

    /** */
    private final boolean enabled;

    /** */
    private volatile TopologyInfo topInfo;

    /** Cache addresses returned by {@link ClientAddressFinder}. */
    private volatile String[] prevHostAddrs;

    /** Previously requested endpoints for topology version. */
    private volatile long prevTopVer = UNKNOWN_TOP_VER;

    /** */
    public ClientDiscoveryContext(ClientConfiguration clientCfg) {
        log = NullLogger.whenNull(clientCfg.getLogger());
        addresses = clientCfg.getAddresses();
        addrFinder = clientCfg.getAddressesFinder();
        enabled = clientCfg.isClusterDiscoveryEnabled();
        reset();
    }

    /** */
    void reset() {
        topInfo = new TopologyInfo(UNKNOWN_TOP_VER, Collections.emptyMap());
        prevTopVer = UNKNOWN_TOP_VER;
        prevHostAddrs = null;
    }

    /**
     * Updates nodes endpoints from the server.
     *
     * @param ch Channel.
     * @return {@code True} if updated.
     */
    boolean refresh(ClientChannel ch) {
        if (addrFinder != null || !enabled)
            return false; // Disabled or custom finder is used.

        if (!ch.protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_GROUP_GET_NODES_ENDPOINTS))
            return false;

        if (ch.serverTopologyVersion() != null && topInfo.topVer >= ch.serverTopologyVersion().topologyVersion()) {
            if (log.isDebugEnabled())
                log.debug("Endpoints information is up to date, no update required");

            return false; // Info is up to date.
        }

        // Allow only one request at time.
        if (refreshIsInProgress.compareAndSet(false, true)) {
            if (log.isDebugEnabled())
                log.debug("Updating nodes endpoints");

            try {
                Map<UUID, NodeInfo> nodes = new HashMap<>(topInfo.nodes);

                TopologyInfo newTopInfo = ch.service(ClientOperation.CLUSTER_GROUP_GET_NODE_ENDPOINTS,
                    req -> {
                        req.out().writeLong(topInfo.topVer);
                        req.out().writeLong(UNKNOWN_TOP_VER);
                    },
                    res -> {
                        try (BinaryReaderExImpl reader = ClientUtils.createBinaryReader(null, res.in())) {
                            long topVer = reader.readLong();

                            // Read added nodes.
                            int nodesAdded = reader.readInt();

                            for (int i = 0; i < nodesAdded; i++) {
                                UUID nodeId = new UUID(reader.readLong(), reader.readLong());
                                int port = reader.readInt();

                                int addrsCnt = reader.readInt();

                                List<String> addrs = new ArrayList<>();

                                for (int j = 0; j < addrsCnt; j++)
                                    addrs.add(reader.readString());

                                nodes.put(nodeId, new NodeInfo(port, addrs));
                            }

                            // Read removed nodes.
                            int nodesRemoved = reader.readInt();

                            for (int i = 0; i < nodesRemoved; i++) {
                                UUID nodeId = new UUID(reader.readLong(), reader.readLong());

                                nodes.remove(nodeId);
                            }

                            return new TopologyInfo(topVer, nodes);
                        }
                        catch (IOException e) {
                            // Only declared for close() method, but never throwed.
                            assert false : "Unexpected exception: " + e;

                            return null;
                        }
                    }
                );

                if (log.isDebugEnabled()) {
                    log.debug("Updated nodes endpoints [topVer=" + newTopInfo.topVer +
                        ", nodesCnt=" + newTopInfo.nodes.size() + ']');
                }

                if (topInfo.topVer < newTopInfo.topVer) {
                    topInfo = newTopInfo;
                    return true;
                }
                else
                    return false;
            }
            finally {
                refreshIsInProgress.set(false);
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Concurrent nodes endpoints update already in progress, skipping");

            return false;
        }
    }

    /**
     * Gets list of endpoins for each node.
     *
     * @return Set of nodes with list of endpoints for each node, or {@code null} if endpoints are not changed
     * since last request.
     */
    @Nullable Set<List<InetSocketAddress>> getEndpoints() {
        Set<List<InetSocketAddress>> endpoints = null;
        TopologyInfo topInfo = this.topInfo;

        if (addrFinder != null || topInfo.topVer == UNKNOWN_TOP_VER) {
            String[] hostAddrs = addrFinder == null ? addresses : addrFinder.getAddresses();

            if (F.isEmpty(hostAddrs))
                throw new ClientException("Empty addresses");

            if (!Arrays.equals(hostAddrs, prevHostAddrs)) {
                endpoints = parsedAddresses(hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        }
        else if (prevTopVer != topInfo.topVer) {
            endpoints = topInfo.endpoints;
            prevTopVer = topInfo.topVer;
        }

        return endpoints;
    }

    /**
     * @return Set of host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static Set<List<InetSocketAddress>> parsedAddresses(String[] addrs) throws ClientException {
        if (F.isEmpty(addrs))
            throw new ClientException("Empty addresses");

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                    a,
                    ClientConnectorConfiguration.DFLT_PORT,
                    ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new ClientException(e);
            }
        }

        return ranges.stream()
            .flatMap(r -> IntStream
                .rangeClosed(r.portFrom(), r.portTo()).boxed()
                .map(p -> Collections.singletonList(InetSocketAddress.createUnresolved(r.host(), p)))
            ).collect(Collectors.toSet());
    }

    /** */
    private static class TopologyInfo {
        /** */
        private final long topVer;

        /** */
        private final Map<UUID, NodeInfo> nodes;

        /** Normalized nodes endpoints. */
        private final Set<List<InetSocketAddress>> endpoints;

        /** */
        private TopologyInfo(long ver, Map<UUID, NodeInfo> nodes) {
            topVer = ver;
            this.nodes = nodes;
            endpoints = normalizeEndpoints(nodes.values());
        }

        /** Remove duplicates from nodes endpoints. */
        private static Set<List<InetSocketAddress>> normalizeEndpoints(Collection<NodeInfo> nodes) {
            Set<List<InetSocketAddress>> endpoints = new HashSet<>();
            Set<InetSocketAddress> used = new HashSet<>();

            for (NodeInfo nodeInfo : nodes) {
                List<InetSocketAddress> addrs = new ArrayList<>(nodeInfo.addrs.size());

                // Check each address of each node for intersection with other nodes addresses.
                for (String host : nodeInfo.addrs) {
                    InetSocketAddress addr = InetSocketAddress.createUnresolved(host, nodeInfo.port);

                    if (used.add(addr))
                        addrs.add(addr);
                }

                if (!addrs.isEmpty())
                    endpoints.add(addrs);
            }

            return endpoints;
        }
    }

    /** */
    private static class NodeInfo {
        /** */
        private final int port;

        /** */
        private final List<String> addrs;

        /** */
        private NodeInfo(int port, List<String> addrs) {
            this.port = port;
            this.addrs = addrs;
        }
    }
}
