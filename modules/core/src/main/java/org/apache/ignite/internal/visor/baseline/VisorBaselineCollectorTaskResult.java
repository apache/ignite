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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for {@link VisorBaselineCollectorTask}.
 */
public class VisorBaselineCollectorTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long topVer;

    /** */
    private List<VisorBaselineNode> baseline;

    /** */
    private List<VisorBaselineNode> others;

    /**
     * Default constructor.
     */
    public VisorBaselineCollectorTaskResult() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param topVer Current topology version.
     * @param baseline Nodes to convert to DTO.
     * @param others Other baseline.
     */
    public VisorBaselineCollectorTaskResult(long topVer, Collection<BaselineNode> baseline, Collection<ClusterNode> others) {
        this.topVer = topVer;

        if (!F.isEmpty(baseline)) {
            this.baseline = new ArrayList<>();
            this.others = new ArrayList<>();

            for (BaselineNode node : baseline)
                this.baseline.add(new VisorBaselineNode(node));

            for (ClusterNode node : others)
                this.others.add(new VisorBaselineNode(node));
        }
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
    public List<VisorBaselineNode> getBaseline() {
        return baseline;
    }

    /**
     * @return Other nodes.
     */
    public List<VisorBaselineNode> getOthers() {
        return others;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(topVer);
        U.writeCollection(out, baseline);
        U.writeCollection(out, others);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = in.readLong();
        baseline = U.readList(in);
        others = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineCollectorTaskResult.class, this);
    }
}
