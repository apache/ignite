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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * This class can be used as a {@link RendezvousAffinityFunction#affinityBackupFilter } to create
 * cache templates in Spring that force each partition's primary and backup to different hardware which
 * is not expected to fail simultaneously, e.g., in AWS, to different "availability zones".  This
 * is a per-partition selection, and different partitions may choose different primaries.
 * <p>
 * This implementation will discard backups rather than place multiple on the same set of nodes. This avoids
 * trying to cram more data onto remaining nodes  when some have failed.
 * <p>
 * A list of node attributes to compare is provided on construction.  Note: "All cluster nodes,
 * on startup, automatically register all the environment and system properties as node attributes."
 * <p>
 * This class is constructed with a array of node attribute names, and a candidate node will be rejected if *any* of the
 * previously selected nodes for a partition have the identical values for *all* of those attributes on the candidate node.
 * Another way to understand this is the set of attribute values defines the key of a group into which a node is placed,
 * an the primaries and backups for a partition cannot share nodes in the same group.   A null attribute is treated  as
 * a distinct value, so two nodes with a null attribute will be treated as having the same value.
 * <p>
 * Warning: the misspelling of an attribute name can cause all nodes to believe they have a null attribute, which would
 * the number of cache entries seen in visor with the number of expected entries, e.g., SELECT COUNT(*) from YOUR_TABLE
 * times the number of backups.
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * Create a partitioned cache template plate with 1 backup, where the backup will not be placed in the same availability zone
 * as the primary.   Note: This example requires that the environment variable "AVAILABILTY_ZONE" be set appropriately on
 * each node via some means external to Ignite.  On AWS, some nodes might have AVAILABILTY_ZONE=us-east-1a and others
 * AVAILABILTY_ZONE=us-east-1b.
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
 *                         &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter"&gt;
 *                             &lt;constructor-arg&gt;
 *                                 &lt;array value-type="java.lang.String"&gt;
 *                                     &lt;!-- Backups must go to different AZs --&gt;
 *                                     &lt;value&gt;AVAILABILITY_ZONE&lt;/value&gt;
 *                                 &lt;/array&gt;
 *                             &lt;/constructor-arg&gt;
 *                         &lt;/bean&gt;
 *                     &lt;/property&gt;
 *                 &lt;/bean&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/list&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * With more backups, multiple properties, e.g., SITE, ZONE,  could be used to force backups to different subgroups.
 */
public class ClusterNodeAttributeAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** */
    private static final long serialVersionUID = 1L;

    /** Attribute names. */
    private final String[] attributeNames;

    /**
     * @param attributeNames The list of attribute names for the set of attributes to compare. Must be at least one.
     */
    ClusterNodeAttributeAffinityBackupFilter(String... attributeNames) {
        A.ensure(attributeNames.length > 0, "attributeNames.length > 0");

        this.attributeNames = attributeNames;
    }

    /**
     * Defines a predicate which returns {@code true} if a node is acceptable for a backup
     * or {@code false} otherwise. An acceptable node is one where its set of attribute values
     * is not exact match with any of the previously selected nodes.  If an attribute does not
     * exist on exactly one node of a pair, then the attribute does not match.  If the attribute
     * does not exist both nodes of a pair, then the attribute matches.
     * <p>
     * Warning:  if an attribute is specified that does not exist on any node, then no backups
     * will be created, because all nodes will match.
     * <p>
     *
     * @param candidate          A node that is a candidate for becoming a backup node for a partition.
     * @param previouslySelected A list of primary/backup nodes already chosen for a partition.
     *                           The primary is first.
     */
    @Override public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        for (ClusterNode node : previouslySelected) {
            boolean match = true;

            for (String attribute : attributeNames) {
                if (!Objects.equals(candidate.attribute(attribute), node.attribute(attribute))) {
                    match = false;

                    break;
                }
            }

            if (match)
                return false;
        }

        return true;
    }

}
