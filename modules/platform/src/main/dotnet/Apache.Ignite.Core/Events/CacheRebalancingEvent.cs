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
        private readonly string cacheName;

        /** */
        private readonly int partition;

        /** */
        private readonly IClusterNode discoveryNode;

        /** */
        private readonly int discoveryEventType;

        /** */
        private readonly string discoveryEventName;

        /** */
        private readonly long discoveryTimestamp;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheRebalancingEvent(IPortableRawReader r) : base(r)
        {
            cacheName = r.ReadString();
            partition = r.ReadInt();
            discoveryNode = ReadNode(r);
            discoveryEventType = r.ReadInt();
            discoveryEventName = r.ReadString();
            discoveryTimestamp = r.ReadLong();
        }
		
        /// <summary>
        /// Gets cache name. 
        /// </summary>
        public string CacheName { get { return cacheName; } }

        /// <summary>
        /// Gets partition for the event. 
        /// </summary>
        public int Partition { get { return partition; } }

        /// <summary>
        /// Gets shadow of the node that triggered this rebalancing event. 
        /// </summary>
        public IClusterNode DiscoveryNode { get { return discoveryNode; } }

        /// <summary>
        /// Gets type of discovery event that triggered this rebalancing event. 
        /// </summary>
        public int DiscoveryEventType { get { return discoveryEventType; } }

        /// <summary>
        /// Gets name of discovery event that triggered this rebalancing event. 
        /// </summary>
        public string DiscoveryEventName { get { return discoveryEventName; } }

        /// <summary>
        /// Gets timestamp of discovery event that caused this rebalancing event. 
        /// </summary>
        public long DiscoveryTimestamp { get { return discoveryTimestamp; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: CacheName={1}, Partition={2}, DiscoveryNode={3}, DiscoveryEventType={4}, " +
	                             "DiscoveryEventName={5}, DiscoveryTimestamp={6}", Name, CacheName, Partition,
	                             DiscoveryNode, DiscoveryEventType, DiscoveryEventName, DiscoveryTimestamp);
	    }
    }
}
