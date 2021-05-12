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

namespace Apache.Ignite.Core.Cache.Affinity.Rendezvous
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Attribute-based affinity backup filter, see <see cref="IBaselineNode.Attributes"/>,
    /// <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>.
    /// <para />
    /// This filter can be used to ensure that, for a given partition, primary and backup nodes are selected from
    /// different racks in a datacenter, or from different availability zones in a cloud environment, so that
    /// a single hardware failure does not cause data loss.
    /// <para />
    /// This implementation will discard backups rather than place multiple on the same set of nodes. This avoids
    /// trying to cram more data onto remaining nodes  when some have failed.
    /// <para />
    /// This class is constructed with a set of node attribute names, and a candidate node will be rejected if
    /// *any* of the previously selected nodes for a partition have identical values for *all* of those attributes
    /// on the candidate node. Another way to understand this is the set of attribute values defines the key of a
    /// group into which a node is placed, and the primaries and backups for a partition cannot share nodes
    /// in the same group. A null attribute is treated as a distinct value, so two nodes with a null attribute will
    /// be treated as having the same value.
    /// <para />
    /// For example, let's say Ignite cluster of 12 nodes is deployed into 3 racks - r1, r2, r3. Ignite nodes
    /// have "SITE" attributes set accordingly to "r1", "r2", "r3". For a cache with 1 backup
    /// (<see cref="CacheConfiguration.Backups"/> set to <c>1</c>), every partition is assigned to 2 nodes.
    /// When the primary node has "SITE" attribute set to "r1", all other nodes with "SITE"="r1" are excluded
    /// by this filter when selecting the backup node.
    /// </summary>
    public sealed class ClusterNodeAttributeAffinityBackupFilter : IAffinityBackupFilter
    {
        /// <summary>
        /// Gets or sets the attribute names.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> AttributeNames { get; set; }
    }
}
