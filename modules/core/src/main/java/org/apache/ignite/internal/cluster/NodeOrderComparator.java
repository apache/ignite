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
 *
 */

package org.apache.ignite.internal.cluster;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Node order comparator.
 */
public class NodeOrderComparator implements Comparator<ClusterNode>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Comparator<ClusterNode> INSTANCE = new NodeOrderComparator();

    /**
     * @return Node comparator.
     */
    public static Comparator<ClusterNode> getInstance() {
        return IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_USE_LEGACY_NODE_COMPARATOR) ?
            NodeOrderLegacyComparator.INSTANCE : INSTANCE;
    }

    /**
     * Private constructor. Don't create this class, use {@link #getInstance()}.
     */
    private NodeOrderComparator() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int compare(ClusterNode n1, ClusterNode n2) {
        Object id1 = n1.consistentId();
        Object id2 = n2.consistentId();

        if (id1 instanceof Comparable && id2 instanceof Comparable && id1.getClass().equals(id2.getClass()))
            return ((Comparable)id1).compareTo(id2);

        return id1.toString().compareTo(id2.toString());
    }
}
