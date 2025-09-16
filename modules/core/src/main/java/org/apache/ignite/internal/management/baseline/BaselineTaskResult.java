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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result for {@link BaselineTask}.
 */
public class BaselineTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cluster state. */
    private ClusterState clusterState;

    /** Current topology version. */
    private long topVer;

    /** Current baseline nodes. */
    private Map<String, BaselineNode> baseline;

    /** Current server nodes. */
    private Map<String, BaselineNode> servers;

    /** Baseline autoadjustment settings. */
    private BaselineAutoAdjustSettings autoAdjustSettings;

    /** Time to next baseline adjust. */
    private long remainingTimeToBaselineAdjust = -1;

    /** Is baseline adjust in progress? */
    private boolean baselineAdjustInProgress = false;

    /**
     * Default constructor.
     */
    public BaselineTaskResult() {
        // No-op.
    }

    /**
     * @param nodes Nodes to process.
     * @return Map of DTO objects.
     */
    private static Map<String, BaselineNode> toMap(Collection<? extends org.apache.ignite.cluster.BaselineNode> nodes) {
        if (F.isEmpty(nodes))
            return null;

        Map<String, BaselineNode> map = new TreeMap<>();

        for (org.apache.ignite.cluster.BaselineNode node : nodes) {
            BaselineNode dto = new BaselineNode(node, Collections.emptyList());

            map.put(dto.getConsistentId(), dto);
        }

        return map;
    }

    /**
     * @param nodes Nodes to process.
     * @return Map of DTO objects, with resolved ip->hostname pairs.
     */
    private static Map<String, BaselineNode> toMapWithResolvedAddresses(
        Collection<? extends org.apache.ignite.cluster.BaselineNode> nodes
    ) {
        if (F.isEmpty(nodes))
            return null;

        Map<String, BaselineNode> map = new TreeMap<>();

        for (org.apache.ignite.cluster.BaselineNode node : nodes) {
            Collection<BaselineNode.ResolvedAddresses> addrs = new ArrayList<>();

            if (node instanceof IgniteClusterNode) {
                for (InetAddress inetAddr: resolveInetAddresses((ClusterNode)node))
                    addrs.add(new BaselineNode.ResolvedAddresses(inetAddr));
            }

            BaselineNode dto = new BaselineNode(node, addrs);

            map.put(dto.getConsistentId(), dto);
        }

        return map;
    }

    /**
     * @return Resolved inet addresses of node
     */
    private static Collection<InetAddress> resolveInetAddresses(ClusterNode node) {
        Set<InetAddress> res = new HashSet<>(node.addresses().size());

        Iterator<String> hostNamesIt = node.hostNames().iterator();

        for (String addr : node.addresses()) {
            String hostName = hostNamesIt.hasNext() ? hostNamesIt.next() : null;

            InetAddress inetAddr = null;

            if (!F.isEmpty(hostName)) {
                try {
                    if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_TEST_ENV)) {
                        // 127.0.0.1.hostname will be resolved to 127.0.0.1
                        if (hostName.endsWith(".hostname")) {
                            String ipStr = hostName.substring(0, hostName.length() - ".hostname".length());
                            inetAddr = InetAddress.getByAddress(hostName, InetAddress.getByName(ipStr).getAddress());
                        }
                    }
                    else
                        inetAddr = InetAddress.getByName(hostName);
                }
                catch (UnknownHostException ignored) {
                }
            }

            if (inetAddr == null || inetAddr.isLoopbackAddress()) {
                try {
                    if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_TEST_ENV))
                        // 127.0.0.1 will be reverse-resolved to 127.0.0.1.hostname
                        inetAddr = InetAddress.getByAddress(addr + ".hostname", InetAddress.getByName(addr).getAddress());
                    else
                        inetAddr = InetAddress.getByName(addr);
                }
                catch (UnknownHostException ignored) {
                }
            }

            if (inetAddr != null)
                res.add(inetAddr);
        }

        return res;
    }

    /**
     * Constructor.
     *
     * @param clusterState Cluster state.
     * @param topVer Current topology version.
     * @param baseline Current baseline nodes.
     * @param servers Current server nodes.
     * @param remainingTimeToBaselineAdjust Time to next baseline adjust.
     * @param baselineAdjustInProgress {@code true} If baseline adjust is in progress.
     */
    public BaselineTaskResult(
        ClusterState clusterState,
        long topVer,
        Collection<? extends org.apache.ignite.cluster.BaselineNode> baseline,
        Collection<? extends org.apache.ignite.cluster.BaselineNode> servers,
        BaselineAutoAdjustSettings autoAdjustSettings,
        long remainingTimeToBaselineAdjust,
        boolean baselineAdjustInProgress) {
        this.clusterState = clusterState;
        this.topVer = topVer;
        this.baseline = toMap(baseline);
        this.servers = toMapWithResolvedAddresses(servers);
        this.autoAdjustSettings = autoAdjustSettings;
        this.remainingTimeToBaselineAdjust = remainingTimeToBaselineAdjust;
        this.baselineAdjustInProgress = baselineAdjustInProgress;
    }

    /**
     * @return Cluster state.
     */
    public ClusterState clusterState() {
        return clusterState;
    }

    /**
     * @return Current topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @return Baseline nodes.
     */
    public Map<String, BaselineNode> getBaseline() {
        return baseline;
    }

    /**
     * @return Server nodes.
     */
    public Map<String, BaselineNode> getServers() {
        return servers;
    }

    /**
     * @return Baseline autoadjustment settings.
     */
    public BaselineAutoAdjustSettings getAutoAdjustSettings() {
        return autoAdjustSettings;
    }

    /**
     * @return Time to next baseline adjust.
     */
    public long getRemainingTimeToBaselineAdjust() {
        return remainingTimeToBaselineAdjust;
    }

    /**
     * @return {@code true} If baseline adjust is in progress.
     */
    public boolean isBaselineAdjustInProgress() {
        return baselineAdjustInProgress;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(topVer);
        U.writeMap(out, baseline);
        U.writeMap(out, servers);
        out.writeObject(autoAdjustSettings);
        out.writeLong(remainingTimeToBaselineAdjust);
        out.writeBoolean(baselineAdjustInProgress);
        U.writeEnum(out, clusterState);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = in.readLong();
        baseline = U.readTreeMap(in);
        servers = U.readTreeMap(in);
        autoAdjustSettings = (BaselineAutoAdjustSettings)in.readObject();
        remainingTimeToBaselineAdjust = in.readLong();
        baselineAdjustInProgress = in.readBoolean();
        clusterState = U.readEnum(in, ClusterState.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineTaskResult.class, this);
    }
}
