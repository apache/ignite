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
        Collection<? extends BaselineNode> servers) {
        this.active = active;
        this.topVer = topVer;
        this.baseline = toMap(baseline);
        this.servers = toMap(servers);
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
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(active);
        out.writeLong(topVer);
        U.writeMap(out, baseline);
        U.writeMap(out, servers);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        active = in.readBoolean();
        topVer = in.readLong();
        baseline = U.readTreeMap(in);
        servers = U.readTreeMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskResult.class, this);
    }
}
