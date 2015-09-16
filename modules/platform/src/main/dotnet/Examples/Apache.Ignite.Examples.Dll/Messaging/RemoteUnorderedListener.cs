/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Messaging;
using GridGain.Resource;

namespace GridGain.Examples.Messaging
{
    /// <summary>
    /// Listener for Unordered topic.
    /// </summary>
    [Serializable]
    public class RemoteUnorderedListener : IMessageFilter<int>
    {
        /** Injected grid instance. */
        [InstanceResource]
        private readonly IGrid _grid;

        /// <summary>
        /// Receives a message and returns a value 
        /// indicating whether provided message and node id satisfy this predicate.
        /// Returning false will unsubscribe this listener from future notifications.
        /// </summary>
        /// <param name="nodeId">Node identifier.</param>
        /// <param name="message">Message.</param>
        /// <returns>Value indicating whether provided message and node id satisfy this predicate.</returns>
        public bool Invoke(Guid nodeId, int message)
        {
            Console.WriteLine("Received unordered message [msg={0}, fromNodeId={1}]", message, nodeId);

            _grid.Cluster.ForNodeIds(nodeId).Message().Send(message, Topic.Unordered);

            return true;
        }
    }
}