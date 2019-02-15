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
    using System.Globalization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// In-memory database (cache) rebalancing event. Rebalance event happens every time there is a change
    /// </summary>
    public sealed class CacheRebalancingEvent : EventBase
	{
        /** */
        private readonly string _cacheName;

        /** */
        private readonly int _partition;

        /** */
        private readonly IClusterNode _discoveryNode;

        /** */
        private readonly int _discoveryEventType;

        /** */
        private readonly string _discoveryEventName;

        /** */
        private readonly long _discoveryTimestamp;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheRebalancingEvent(IBinaryRawReader r) : base(r)
        {
            _cacheName = r.ReadString();
            _partition = r.ReadInt();
            _discoveryNode = ReadNode(r);
            _discoveryEventType = r.ReadInt();
            _discoveryEventName = r.ReadString();
            _discoveryTimestamp = r.ReadLong();
        }
		
        /// <summary>
        /// Gets cache name. 
        /// </summary>
        public string CacheName { get { return _cacheName; } }

        /// <summary>
        /// Gets partition for the event. 
        /// </summary>
        public int Partition { get { return _partition; } }

        /// <summary>
        /// Gets shadow of the node that triggered this rebalancing event. 
        /// </summary>
        public IClusterNode DiscoveryNode { get { return _discoveryNode; } }

        /// <summary>
        /// Gets type of discovery event that triggered this rebalancing event. 
        /// </summary>
        public int DiscoveryEventType { get { return _discoveryEventType; } }

        /// <summary>
        /// Gets name of discovery event that triggered this rebalancing event. 
        /// </summary>
        public string DiscoveryEventName { get { return _discoveryEventName; } }

        /// <summary>
        /// Gets timestamp of discovery event that caused this rebalancing event. 
        /// </summary>
        public long DiscoveryTimestamp { get { return _discoveryTimestamp; } }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
	    public override string ToShortString()
	    {
	        return string.Format(CultureInfo.InvariantCulture,
	            "{0}: CacheName={1}, Partition={2}, DiscoveryNode={3}, DiscoveryEventType={4}, " +
	            "DiscoveryEventName={5}, DiscoveryTimestamp={6}", Name, CacheName, Partition,
	            DiscoveryNode, DiscoveryEventType, DiscoveryEventName, DiscoveryTimestamp);
	    }
    }
}
