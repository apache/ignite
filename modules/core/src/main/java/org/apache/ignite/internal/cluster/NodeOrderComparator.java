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
