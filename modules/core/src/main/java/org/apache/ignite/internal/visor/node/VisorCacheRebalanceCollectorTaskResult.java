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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result object for {@link VisorCacheRebalanceCollectorTask} task.
 */
public class VisorCacheRebalanceCollectorTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Rebalance state on nodes. */
    private Map<UUID, Double> rebalance = new HashMap<>();

    /** Nodes baseline status. */
    private Map<UUID, VisorNodeBaselineStatus> baseline = new HashMap<>();

    /**
     * Default constructor.
     */
    public VisorCacheRebalanceCollectorTaskResult() {
        // No-op.
    }

    /**
     * @return Rebalance on nodes.
     */
    public Map<UUID, Double> getRebalance() {
        return rebalance;
    }

    /**
     * @return Baseline.
     */
    public Map<UUID, VisorNodeBaselineStatus> getBaseline() {
        return baseline;
    }

    /**
     * Add specified results.
     *
     * @param res Results to add.
     */
    public void add(VisorCacheRebalanceCollectorTaskResult res) {
        assert res != null;

        rebalance.putAll(res.getRebalance());
        baseline.putAll(res.getBaseline());
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, rebalance);
        U.writeMap(out, baseline);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        rebalance = U.readMap(in);
        baseline = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheRebalanceCollectorTaskResult.class, this);
    }
}
