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

package com.sbt.sbergrid.extras;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * This class can be used as a {@link RendezvousAffinityFunction#setAffinityBackupFilter } to create
 * cache templates in Spring that force each partition's primary and backup to be co-located on nodes with the same
 * attribute value.
 * <p>
 * This implementation will discard backups rather than place copies on nodes with different attribute values. This
 * avoids trying to cram more data onto remaining nodes when some have failed.
 * <p>
 * A node attribute to compare is provided on construction.  Note: "All cluster nodes,
 * on startup, automatically register all the environment and system properties as node attributes."
 * <p>
 * This class is constructed with a node attribute name, and a candidate node will be rejected if previously selected
 * nodes for a partition have a different value for attribute on the candidate node.
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * Create a partitioned cache template plate with 1 backup, where the backup will be placed in the same cell
 * as the primary.   Note: This example requires that the environment variable "CELL" be set appropriately on
 * each node via some means external to Ignite.
 * <pre name="code" class="xml">
 * &lt;property name="cacheConfiguration"&gt;
 *     &lt;list&gt;
 *         &lt;bean id="cache-template-bean" abstract="true" class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *             &lt;property name="name" value="JobcaseDefaultCacheConfig*"/&gt;
 *             &lt;property name="cacheMode" value="PARTITIONED" /&gt;
 *             &lt;property name="backups" value="1" /&gt;
 *             &lt;property name="affinity"&gt;
 *                 &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction"&gt;
 *                     &lt;property name="affinityBackupFilter"&gt;
 *                         &lt;bean class="com.sbt.sbergrid.extras.ClusterNodeAttributeColocatedBackupFilter"&gt;
 *                             &lt;!-- Backups must go to the same CELL as primary --&gt;
 *                             &lt;constructor-arg value="CELL" /&gt;
 *                         &lt;/bean&gt;
 *                     &lt;/property&gt;
 *                 &lt;/bean&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/list&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 *
 * @deprecated Use {@link org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilter}
 * instead.
 */
@Deprecated
public class ClusterNodeAttributeColocatedBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** */
    private static final long serialVersionUID = 1L;

    /** Attribute name. */
    private final String attributeName;

    /**
     * @param attributeName The attribute name for the attribute to compare.
     */
    public ClusterNodeAttributeColocatedBackupFilter(String attributeName) {
        this.attributeName = attributeName;
    }

    /**
     * Defines a predicate which returns {@code true} if a node is acceptable for a backup
     * or {@code false} otherwise. An acceptable node is one where its attribute value
     * is exact match with previously selected nodes.  If an attribute does not
     * exist on exactly one node of a pair, then the attribute does not match.  If the attribute
     * does not exist both nodes of a pair, then the attribute matches.
     *
     * @param candidate          A node that is a candidate for becoming a backup node for a partition.
     * @param previouslySelected A list of primary/backup nodes already chosen for a partition.
     *                           The primary is first.
     */
    @Override public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        for (ClusterNode node : previouslySelected)
            return Objects.equals(candidate.attribute(attributeName), node.attribute(attributeName));

        return true;
    }
}
