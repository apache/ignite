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
