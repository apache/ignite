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

package org.apache.ignite.internal.processors.rest.handlers.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.stream.Collectors.toList;

/**
 * Result for baseline command.
 */
public class GridBaselineCommandResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cluster state. */
    private boolean active;

    /** Current topology version. */
    private long topVer;

    /** Current baseline nodes. */
    private Collection<String> baseline;

    /** Current server nodes. */
    private Collection<String> srvs;

    /**
     * @param nodes Nodes to process.
     * @return Collection of consistentIds.
     */
    private static Collection<String> consistentIds(Collection<? extends BaselineNode> nodes) {
        return nodes.stream().map(n -> String.valueOf(n.consistentId())).collect(toList());
    }

    /**
     * Default constructor.
     */
    public GridBaselineCommandResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param active Cluster state.
     * @param topVer Current topology version.
     * @param baseline Current baseline nodes.
     * @param srvs Current server nodes.
     */
    GridBaselineCommandResponse(
        boolean active,
        long topVer,
        Collection<? extends BaselineNode> baseline,
        Collection<? extends BaselineNode> srvs
    ) {
        this.active = active;
        this.topVer = topVer;
        this.baseline = consistentIds(baseline);
        this.srvs = consistentIds(srvs);
    }

    /**
     * @return Cluster state.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @param active Cluster active.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return Current topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Current topology version.
     */
    public void setTopologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Baseline nodes.
     */
    public Collection<String> getBaseline() {
        return baseline;
    }

    /**
     * @param baseline Baseline nodes.
     */
    public void setBaseline(Collection<String> baseline) {
        this.baseline = baseline;
    }

    /**
     * @return Server nodes.
     */
    public Collection<String> getServers() {
        return srvs;
    }

    /**
     * @param srvs Server nodes.
     */
    public void setServers(Collection<String> srvs) {
        this.srvs = srvs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(active);
        out.writeLong(topVer);
        U.writeCollection(out, baseline);
        U.writeCollection(out, srvs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        active = in.readBoolean();
        topVer = in.readLong();
        baseline = U.readCollection(in);
        srvs = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridBaselineCommandResponse.class, this);
    }
}
