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

package org.apache.ignite.internal.processors.platform.cluster;

import java.util.Collection;
import java.util.UUID;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterGroupEx;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

/**
 * Interop projection.
 */
@SuppressWarnings({"UnusedDeclaration"})
public class PlatformClusterGroup extends PlatformAbstractTarget {
    /** */
    private static final int OP_FOR_ATTRIBUTE = 2;

    /** */
    private static final int OP_FOR_CACHE = 3;

    /** */
    private static final int OP_FOR_CLIENT = 4;

    /** */
    private static final int OP_FOR_DATA = 5;

    /** */
    private static final int OP_FOR_HOST = 6;

    /** */
    private static final int OP_FOR_NODE_IDS = 7;

    /** */
    private static final int OP_METRICS = 9;

    /** */
    private static final int OP_METRICS_FILTERED = 10;

    /** */
    private static final int OP_NODE_METRICS = 11;

    /** */
    private static final int OP_NODES = 12;

    /** */
    private static final int OP_PING_NODE = 13;

    /** */
    private static final int OP_TOPOLOGY = 14;

    /** */
    private static final int OP_FOR_OTHERS = 16;

    /** */
    private static final int OP_FOR_REMOTES = 17;

    /** */
    private static final int OP_FOR_DAEMONS = 18;

    /** */
    private static final int OP_FOR_RANDOM = 19;

    /** */
    private static final int OP_FOR_OLDEST = 20;

    /** */
    private static final int OP_FOR_YOUNGEST = 21;

    /** */
    private static final int OP_RESET_METRICS = 22;

    /** */
    private static final int OP_FOR_SERVERS = 23;

    /** */
    private static final int OP_CACHE_METRICS = 24;

    /** Projection. */
    private final ClusterGroupEx prj;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param prj Projection.
     */
    public PlatformClusterGroup(PlatformContext platformCtx, ClusterGroupEx prj) {
        super(platformCtx);

        this.prj = prj;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_METRICS:
                platformCtx.writeClusterMetrics(writer, prj.metrics());

                break;

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "deprecation"})
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_METRICS_FILTERED: {
                Collection<UUID> ids = PlatformUtils.readCollection(reader);

                platformCtx.writeClusterMetrics(writer, prj.forNodeIds(ids).metrics());

                break;
            }

            case OP_NODES: {
                long oldTopVer = reader.readLong();

                long curTopVer = platformCtx.kernalContext().discovery().topologyVersion();

                if (curTopVer > oldTopVer) {
                    writer.writeBoolean(true);

                    writer.writeLong(curTopVer);

                    // At this moment topology version might have advanced, and due to this race
                    // we return outdated top ver to the callee. But this race is benign, the only
                    // possible side effect is that the user will re-request nodes and we will return
                    // the same set of nodes but with more recent topology version.
                    Collection<ClusterNode> nodes = prj.nodes();

                    platformCtx.writeNodes(writer, nodes);
                }
                else
                    // No discovery events since last invocation.
                    writer.writeBoolean(false);

                break;
            }

            case OP_NODE_METRICS: {
                UUID nodeId = reader.readUuid();

                long lastUpdateTime = reader.readLong();

                // Ask discovery because node might have been filtered out of current projection.
                ClusterNode node = platformCtx.kernalContext().discovery().node(nodeId);

                ClusterMetrics metrics = null;

                if (node != null) {
                    ClusterMetrics metrics0 = node.metrics();

                    long triggerTime = lastUpdateTime + platformCtx.kernalContext().config().getMetricsUpdateFrequency();

                    metrics = metrics0.getLastUpdateTime() > triggerTime ? metrics0 : null;
                }

                platformCtx.writeClusterMetrics(writer, metrics);

                break;
            }

            case OP_TOPOLOGY: {
                long topVer = reader.readLong();

                platformCtx.writeNodes(writer, topology(topVer));

                break;
            }

            case OP_CACHE_METRICS: {
                String cacheName = reader.readString();

                IgniteCache cache = platformCtx.kernalContext().grid().cache(cacheName);

                PlatformCache.writeCacheMetrics(writer, cache.metrics(prj));

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_PING_NODE:
                return pingNode(reader.readUuid()) ? TRUE : FALSE;

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_FOR_NODE_IDS: {
                Collection<UUID> ids = PlatformUtils.readCollection(reader);

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forNodeIds(ids));
            }

            case OP_FOR_ATTRIBUTE:
                return new PlatformClusterGroup(platformCtx,
                    (ClusterGroupEx)prj.forAttribute(reader.readString(), reader.readString()));

            case OP_FOR_CACHE: {
                String cacheName = reader.readString();

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forCacheNodes(cacheName));
            }

            case OP_FOR_CLIENT: {
                String cacheName = reader.readString();

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forClientNodes(cacheName));
            }

            case OP_FOR_DATA: {
                String cacheName = reader.readString();

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forDataNodes(cacheName));
            }

            case OP_FOR_HOST: {
                UUID nodeId = reader.readUuid();

                ClusterNode node = prj.node(nodeId);

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx) prj.forHost(node));
            }

            default:
                return super.processInStreamOutObject(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(
            int type, @Nullable PlatformTarget arg, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        switch (type) {
            case OP_FOR_OTHERS: {
                PlatformClusterGroup exclude = (PlatformClusterGroup) arg;

                assert exclude != null;

                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forOthers(exclude.prj));
            }
        }

        return super.processInObjectStreamOutObjectStream(type, arg, reader, writer);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_FOR_REMOTES:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forRemotes());

            case OP_FOR_DAEMONS:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forDaemons());

            case OP_FOR_RANDOM:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forRandom());

            case OP_FOR_OLDEST:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forOldest());

            case OP_FOR_YOUNGEST:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forYoungest());

            case OP_FOR_SERVERS:
                return new PlatformClusterGroup(platformCtx, (ClusterGroupEx)prj.forServers());
        }

        return super.processOutObject(type);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_RESET_METRICS: {
                assert prj instanceof IgniteCluster; // Can only be invoked on top-level cluster group.

                ((IgniteCluster)prj).resetMetrics();

                return TRUE;
            }
        }

        return super.processInLongOutLong(type, val);
    }

    /**
     * @return Projection.
     */
    public ClusterGroupEx projection() {
        return prj;
    }

    /**
     * Pings a remote node.
     */
    private boolean pingNode(UUID nodeId) {
        assert prj instanceof IgniteCluster; // Can only be invoked on top-level cluster group.

        return ((IgniteCluster)prj).pingNode(nodeId);
    }

    /**
     * Gets a topology by version. Returns {@code null} if topology history storage doesn't contain
     * specified topology version (history currently keeps last {@code 1000} snapshots).
     *
     * @param topVer Topology version.
     * @return Collection of grid nodes which represented by specified topology version,
     * if it is present in history storage, {@code null} otherwise.
     * @throws UnsupportedOperationException If underlying SPI implementation does not support
     *      topology history. Currently only {@link TcpDiscoverySpi}
     *      supports topology history.
     */
    private Collection<ClusterNode> topology(long topVer) {
        assert prj instanceof IgniteCluster; // Can only be invoked on top-level cluster group.

        return ((IgniteCluster)prj).topology(topVer);
    }
}
