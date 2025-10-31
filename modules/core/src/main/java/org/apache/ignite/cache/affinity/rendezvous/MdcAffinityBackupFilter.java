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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Multi-data center affinity backup filter that ensures each partition's data is distributed across multiple data centers,
 * providing high availability and fault tolerance. This implementation guarantees at least one copy of the data in each
 * data center and attempts to maintain the configured backup factor without discarding copies.
 * <p>
 * The filter works by grouping nodes based on their data center identification attribute (@see {@link ClusterNode#dataCenterId()})
 * and ensuring that for every partition, at least one node from each data center is included in the primary-backup set.
 * <p>
 * The filter will discard backup copies only if the number of available nodes in a given data center is less
 * than the number of copies assigned to that data center.
 * For example, if a partition has 4 copies (1 primary and 3 backups) and the cluster has 2 data centers,
 * than 2 copies are assigned to each data center. The only scenario when just a single copy is assigned to a node in a data center is when
 * the number of nodes in that data center is one.
 * <p>
 * This class is constructed with a number of data centers the cluster spans and a number of backups of the cache this filter is applied to.
 * Implementation expects that all copies can be spread evenly across all data centers. In other words, (backups + 1) is divisible by
 * number of data centers without remainder. Uneven distributions of copies are not supported.
 * <p>
 * Warning: Ensure that all nodes have a consistent and valid data center identifier attribute. Missing or inconsistent values
 * may lead to unexpected placement of data.
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * Create a partitioned cache template where each data center has at least one copy of the data, and the backup count is maintained.
 * <pre name="code" class="xml">
 * &lt;property name="cacheConfiguration"&gt;
 *     &lt;list&gt;
 *         &lt;bean id="cache-template-bean" abstract="true" class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *             &lt;property name="name" value="JobcaseDefaultCacheConfig*"/&gt;
 *             &lt;property name="cacheMode" value="PARTITIONED" /&gt;
 *             &lt;property name="backups" value="3" /&gt;
 *             &lt;property name="affinity"&gt;
 *                 &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction"&gt;
 *                     &lt;property name="affinityBackupFilter"&gt;
 *                         &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.MdcAffinityBackupFilter"&gt;
 *                             &lt;constructor-arg value="2"/&gt; <!-- dcsNumber -->
 *                             &lt;constructor-arg value="3"/&gt; <!-- backups, the same as in the cache template -->
 *                         &lt;/bean&gt;
 *                     &lt;/property&gt;
 *                 &lt;/bean&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/list&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * With more backups, additional replicas can be distributed across different data centers to further improve redundancy.
 */
public class MdcAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** */
    private static final long serialVersionUID = 1L;

    /** */
    private final int partCopiesPerDc;

    /**
     * @param dcsNum Number of data centers.
     * @param backups Number of backups.
     */
    public MdcAffinityBackupFilter(int dcsNum, int backups) {
        if (dcsNum < 2) {
            throw new IllegalArgumentException("MdcAffinityBackupFilter cannot be used in an environment with only one datacenter. " +
                "Number of datacenters must be at least 2.");
        }

        int numCopies = backups + 1;

        partCopiesPerDc = numCopies / dcsNum;
        int remainder = numCopies % dcsNum;

        if (remainder != 0) {
            String suggestion = "recommended ";
            if (numCopies - remainder <= 0)
                suggestion += "value is " + (backups + (dcsNum - remainder));
            else
                suggestion += "values are " + (backups - remainder) + " and " + (backups + (dcsNum - remainder));
            
            throw new IllegalArgumentException("Number of copies is not completely divisible by number of datacenters, " +
                "copies cannot be distributed evenly across DCs. " +
                "Please adjust the number of backups, " + suggestion);
        }


    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        String candidateDcId = candidate.dataCenterId();
        int candDcCopiesAssigned = 0;

        for (int i = 0; i < previouslySelected.size(); i++) {
            String prevDcId = previouslySelected.get(i).dataCenterId();

            if (prevDcId == null)
                return false;

            candDcCopiesAssigned += prevDcId.equals(candidateDcId) ? 1 : 0;
        }

        return candDcCopiesAssigned < partCopiesPerDc;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof MdcAffinityBackupFilter))
            return false;

        MdcAffinityBackupFilter filter = (MdcAffinityBackupFilter)o;

        return partCopiesPerDc == filter.partCopiesPerDc;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Integer.hashCode(partCopiesPerDc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MdcAffinityBackupFilter.class, this);
    }
}
