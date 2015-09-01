/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Events
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;

    using GridGain.Cluster;
    using GridGain.Portable;

    using U = GridGain.Impl.GridUtils;

	/// <summary>
    /// Grid discovery event.
    /// </summary>
    public sealed class DiscoveryEvent : EventBase
	{
        /** */
        private readonly IClusterNode eventNode;

        /** */
        private readonly long topologyVersion;

        /** */
        private readonly ReadOnlyCollection<IClusterNode> topologyNodes;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal DiscoveryEvent(IPortableRawReader r) : base(r)
        {
            eventNode = ReadNode(r);
            topologyVersion = r.ReadLong();

            var nodes = U.ReadNodes(r);

            topologyNodes = nodes == null ? null : new ReadOnlyCollection<IClusterNode>(nodes);
        }

        /// <summary>
        /// Gets node that caused this event to be generated. It is potentially different from the node on which this 
        /// event was recorded. For example, node A locally recorded the event that a remote node B joined the topology. 
        /// In this case this method will return ID of B. 
        /// </summary>
        public IClusterNode EventNode { get { return eventNode; } }

        /// <summary>
        /// Gets topology version if this event is raised on topology change and configured discovery
        /// SPI implementation supports topology versioning.
        /// </summary>
        public long TopologyVersion { get { return topologyVersion; } }

        /// <summary>
        /// Gets topology nodes from topology snapshot. If SPI implementation does not support versioning, the best 
        /// effort snapshot will be captured. 
        /// </summary>
        public ICollection<IClusterNode> TopologyNodes { get { return topologyNodes; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: EventNode={1}, TopologyVersion={2}, TopologyNodes={3}", Name, EventNode, 
                TopologyVersion, TopologyNodes.Count);
	    }
    }
}
