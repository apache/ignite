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

package org.apache.ignite.internal.visor.baseline;

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
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for {@link VisorBaselineTask}.
 */
public class VisorBaselineTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cluster state. */
    private boolean active;

    /** Current topology version. */
    private long topVer;

    /** Current baseline nodes. */
    private Map<String, VisorBaselineNode> baseline;

    /** Current server nodes. */
    private Map<String, VisorBaselineNode> servers;

    /** Baseline autoadjustment settings. */
    private VisorBaselineAutoAdjustSettings autoAdjustSettings;

    /** Time to next baseline adjust. */
    private long remainingTimeToBaselineAdjust = -1;

    /** Is baseline adjust in progress? */
    private boolean baselineAdjustInProgress = false;

    /**
     * Default constructor.
     */
    public VisorBaselineTaskResult() {
        // No-op.
    }

    /**
     * @param nodes Nodes to process.
     * @return Map of DTO objects.
     */
    private static Map<String, VisorBaselineNode> toMap(Collection<? extends BaselineNode> nodes) {
        if (F.isEmpty(nodes))
            return null;

        Map<String, VisorBaselineNode> map = new TreeMap<>();

        for (BaselineNode node : nodes) {
            VisorBaselineNode dto = new VisorBaselineNode(node, Collections.emptyList());

            map.put(dto.getConsistentId(), dto);
        }

        return map;
    }

    /**
     * @param nodes Nodes to process.
     * @return Map of DTO objects, with resolved ip->hostname pairs.
     */
    private static Map<String, VisorBaselineNode> toMapWithResolvedAddresses(Collection<? extends BaselineNode> nodes) {
        if (F.isEmpty(nodes))
            return null;

        Map<String, VisorBaselineNode> map = new TreeMap<>();

        for (BaselineNode node : nodes) {
            Collection<VisorBaselineNode.ResolvedAddresses> addrs = new ArrayList<>();

            if (node instanceof IgniteClusterNode) {
                for (InetAddress inetAddress: resolveInetAddresses((ClusterNode)node))
                    addrs.add(new VisorBaselineNode.ResolvedAddresses(inetAddress));
            }

            VisorBaselineNode dto = new VisorBaselineNode(node, addrs);

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
     * @param active Cluster state.
     * @param topVer Current topology version.
     * @param baseline Current baseline nodes.
     * @param servers Current server nodes.
     * @param remainingTimeToBaselineAdjust Time to next baseline adjust.
     * @param baselineAdjustInProgress {@code true} If baseline adjust is in progress.
     */
    public VisorBaselineTaskResult(
        boolean active,
        long topVer,
        Collection<? extends BaselineNode> baseline,
        Collection<? extends BaselineNode> servers,
        VisorBaselineAutoAdjustSettings autoAdjustSettings,
        long remainingTimeToBaselineAdjust,
        boolean baselineAdjustInProgress) {
        this.active = active;
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
    public boolean isActive() {
        return active;
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
    public Map<String, VisorBaselineNode> getBaseline() {
        return baseline;
    }

    /**
     * @return Server nodes.
     */
    public Map<String, VisorBaselineNode> getServers() {
        return servers;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /**
     * @return Baseline autoadjustment settings.
     */
    public VisorBaselineAutoAdjustSettings getAutoAdjustSettings() {
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
        out.writeBoolean(active);
        out.writeLong(topVer);
        U.writeMap(out, baseline);
        U.writeMap(out, servers);
        out.writeObject(autoAdjustSettings);
        out.writeLong(remainingTimeToBaselineAdjust);
        out.writeBoolean(baselineAdjustInProgress);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        active = in.readBoolean();
        topVer = in.readLong();
        baseline = U.readTreeMap(in);
        servers = U.readTreeMap(in);

        if (protoVer > V1) {
            autoAdjustSettings = (VisorBaselineAutoAdjustSettings)in.readObject();
            remainingTimeToBaselineAdjust = in.readLong();
            baselineAdjustInProgress = in.readBoolean();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskResult.class, this);
    }
}
