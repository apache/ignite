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

namespace Apache.Ignite.Core.Events
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Globalization;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Grid discovery event.
    /// </summary>
    public sealed class DiscoveryEvent : EventBase
	{
        /** */
        private readonly IClusterNode _eventNode;

        /** */
        private readonly long _topologyVersion;

        /** */
        private readonly ReadOnlyCollection<IClusterNode> _topologyNodes;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal DiscoveryEvent(BinaryReader r) : base(r)
        {
            _eventNode = ReadNode(r);
            _topologyVersion = r.ReadLong();

            var nodes = IgniteUtils.ReadNodes(r);

            _topologyNodes = nodes == null ? null : new ReadOnlyCollection<IClusterNode>(nodes);
        }

        /// <summary>
        /// Gets node that caused this event to be generated. It is potentially different from the node on which this 
        /// event was recorded. For example, node A locally recorded the event that a remote node B joined the topology. 
        /// In this case this method will return ID of B. 
        /// </summary>
        public IClusterNode EventNode { get { return _eventNode; } }

        /// <summary>
        /// Gets topology version if this event is raised on topology change and configured discovery
        /// SPI implementation supports topology versioning.
        /// </summary>
        public long TopologyVersion { get { return _topologyVersion; } }

        /// <summary>
        /// Gets topology nodes from topology snapshot. If SPI implementation does not support versioning, the best 
        /// effort snapshot will be captured. 
        /// </summary>
        public ICollection<IClusterNode> TopologyNodes { get { return _topologyNodes; } }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
	    public override string ToShortString()
	    {
            return string.Format(CultureInfo.InvariantCulture, 
                "{0}: EventNode={1}, TopologyVersion={2}, TopologyNodes={3}", Name, EventNode, 
                TopologyVersion, TopologyNodes.Count);
	    }
    }
}
