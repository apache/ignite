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

package org.apache.ignite.internal.cluster;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Node order comparator.
 */
public class NodeOrderLegacyComparator implements Comparator<ClusterNode>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final Comparator<ClusterNode> INSTANCE = new NodeOrderLegacyComparator();

    /**
     * Private constructor. Don't create this class, use {@link #INSTANCE}.
     */
    private NodeOrderLegacyComparator() {

    }

    /** {@inheritDoc} */
    @Override public int compare(ClusterNode n1, ClusterNode n2) {
        return n1.order() < n2.order() ? -1 : n1.order() > n2.order() ? 1 : n1.id().compareTo(n2.id());
    }
}