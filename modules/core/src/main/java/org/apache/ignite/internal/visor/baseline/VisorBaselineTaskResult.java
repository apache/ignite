/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.cluster.BaselineNode;
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
            VisorBaselineNode dto = new VisorBaselineNode(node);

            map.put(dto.getConsistentId(), dto);
        }

        return map;
    }

    /**
     * Constructor.
     *
     * @param active Cluster state.
     * @param topVer Current topology version.
     * @param baseline Current baseline nodes.
     * @param servers Current server nodes.
     */
    public VisorBaselineTaskResult(
        boolean active,
        long topVer,
        Collection<? extends BaselineNode> baseline,
        Collection<? extends BaselineNode> servers,
        VisorBaselineAutoAdjustSettings autoAdjustSettings
    ) {
        this.active = active;
        this.topVer = topVer;
        this.baseline = toMap(baseline);
        this.servers = toMap(servers);
        this.autoAdjustSettings = autoAdjustSettings;
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(active);
        out.writeLong(topVer);
        U.writeMap(out, baseline);
        U.writeMap(out, servers);
        out.writeObject(autoAdjustSettings);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        active = in.readBoolean();
        topVer = in.readLong();
        baseline = U.readTreeMap(in);
        servers = U.readTreeMap(in);

        if (protoVer > V1)
            autoAdjustSettings = (VisorBaselineAutoAdjustSettings)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskResult.class, this);
    }
}
