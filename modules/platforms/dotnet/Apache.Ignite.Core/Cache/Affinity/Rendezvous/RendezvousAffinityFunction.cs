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
    using System;

    /// <summary>
    /// Affinity function for partitioned cache based on Highest Random Weight algorithm.
    /// </summary>
    // Actual implementation of this class is in Java, see AffinityFunctionSerializer.Write method.
    [Serializable]
    public class RendezvousAffinityFunction : AffinityFunctionBase
    {
        /// <summary>
        /// Gets or sets an optional backup filter. If provided, then backups will be selected from all nodes
        /// that pass this filter. First node being passed to this filter is a node being tested,
        /// and the second parameter is a list of nodes that are already assigned for a given partition
        /// (primary node is the first in the list).
        /// <para />
        /// Note that <see cref="AffinityBackupFilter"/> is ignored when
        /// <see cref="AffinityFunctionBase.ExcludeNeighbors"/> is <c>true</c>.
        /// <para />
        /// Only one predefined implementation is supported for now:
        /// <see cref="ClusterNodeAttributeAffinityBackupFilter"/>.
        /// </summary>
        public IAffinityBackupFilter AffinityBackupFilter { get; set; }
    }
}