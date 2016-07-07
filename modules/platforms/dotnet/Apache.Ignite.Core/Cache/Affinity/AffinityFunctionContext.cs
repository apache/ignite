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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;

    /// <summary>
    /// Affinity function context.
    /// </summary>
    public class AffinityFunctionContext
    {
        /** */
        private readonly List<List<IClusterNode>> _previousAssignment;

        /** */
        private readonly int _backups;

        /** */
        private readonly ICollection<IClusterNode> _currentTopologySnapshot;

        /** */
        private readonly AffinityTopologyVersion _currentTopologyVersion;

        /** */
        private readonly DiscoveryEvent _discoveryEvent;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionContext"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal AffinityFunctionContext(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            _currentTopologySnapshot = IgniteUtils.ReadNodes(reader);
            _backups = reader.ReadInt();
            _currentTopologyVersion = new AffinityTopologyVersion(reader.ReadLong(), reader.ReadInt());
            _discoveryEvent = EventReader.Read<DiscoveryEvent>(reader);

            // Prev assignment
            var cnt = reader.ReadInt();

            if (cnt > 0)
            {
                _previousAssignment = new List<List<IClusterNode>>(cnt);

                for (var i = 0; i < cnt; i++)
                    _previousAssignment.Add(IgniteUtils.ReadNodes(reader));
            }
        }

        /// <summary>
        /// Gets the affinity assignment for given partition on previous topology version.
        /// First node in returned list is a primary node, other nodes are backups.
        /// </summary>
        /// <param name="partition">The partition to get previous assignment for.</param>
        /// <returns>
        /// List of nodes assigned to a given partition on previous topology version or <code>null</code>
        /// if this information is not available.
        /// </returns>
        public ICollection<IClusterNode> GetPreviousAssignment(int partition)
        {
            return _previousAssignment == null ? null : _previousAssignment[partition];
        }

        /// <summary>
        /// Gets number of backups for new assignment.
        /// </summary>
        public int Backups
        {
            get { return _backups; }
        }

        /// <summary>
        /// Gets the current topology snapshot. Snapshot will contain only nodes on which the particular
        /// cache is configured. List of passed nodes is guaranteed to be sorted in a same order
        /// on all nodes on which partition assignment is performed.
        /// </summary>
        public ICollection<IClusterNode> CurrentTopologySnapshot
        {
            get { return _currentTopologySnapshot; }
        }

        /// <summary>
        /// Gets the current topology version.
        /// </summary>
        public AffinityTopologyVersion CurrentTopologyVersion
        {
            get { return _currentTopologyVersion; }
        }

        /// <summary>
        /// Gets the discovery event that caused the topology change.
        /// </summary>
        public DiscoveryEvent DiscoveryEvent
        {
            get { return _discoveryEvent; }
        }
    }
}