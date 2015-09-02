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

namespace Apache.Ignite.Core.Events
{
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Portable;

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
        internal CacheRebalancingEvent(IPortableRawReader r) : base(r)
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

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: CacheName={1}, Partition={2}, DiscoveryNode={3}, DiscoveryEventType={4}, " +
	                             "DiscoveryEventName={5}, DiscoveryTimestamp={6}", Name, CacheName, Partition,
	                             DiscoveryNode, DiscoveryEventType, DiscoveryEventName, DiscoveryTimestamp);
	    }
    }
}
