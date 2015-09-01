/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
