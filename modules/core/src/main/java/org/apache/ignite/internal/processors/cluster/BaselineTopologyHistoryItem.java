/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class BaselineTopologyHistoryItem implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final int id;

    /** */
    private final Collection<Object> consIds;

    /** */
    private final List<Long> branchingHistory;

    /**
     * @param id Id.
     * @param consIds Consistent IDs.
     * @param branchingHistory Activation history.
     */
    private BaselineTopologyHistoryItem(int id, Collection<Object> consIds, List<Long> branchingHistory) {
        this.id = id;
        this.consIds = consIds;
        this.branchingHistory = branchingHistory;
    }

    /**
     * @param blt Baseline Topology.
     */
    public static BaselineTopologyHistoryItem fromBaseline(BaselineTopology blt) {
        if (blt == null)
            return null;

        List<Long> fullActivationHistory = new ArrayList<>(blt.branchingHistory().size());

        fullActivationHistory.addAll(blt.branchingHistory());

        return new BaselineTopologyHistoryItem(blt.id(), U.arrayList(blt.consistentIds()), fullActivationHistory);
    }

    /**
     * @return ID.
     */
    public int id() {
        return id;
    }

    /**
     *
     */
    public List<Long> branchingHistory() {
        return branchingHistory;
    }

    /**
     * Returns {@code true} if baseline topology history item contains node with given consistent ID.
     *
     * @param consistentId Consistent ID.
     * @return {@code True} if baseline topology history item contains node with given consistent ID.
     */
    public boolean containsNode(Object consistentId) {
        return consIds.contains(consistentId);
    }

    /**
     * Returns a copy of consistent ids of nodes that included into this baseline topology item.
     *
     * @return Collection of consistent ids.
     */
    public Collection<Object> consistentIds() {
        return U.arrayList(this.consIds);
    }
}
