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
 */

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;

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
        internal AffinityFunctionContext(BinaryReader reader)
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