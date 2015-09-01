/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Messaging
{
    using System;
    using System.Collections;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Provides functionality for topic-based message exchange among nodes defined by <see cref="IClusterGroup"/>.
    /// Users can send ordered and unordered messages to various topics. Note that same topic name
    /// cannot be reused between ordered and unordered messages.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IMessaging : IAsyncSupport<IMessaging>
    {
        /// <summary>
        /// Gets the cluster group to which this instance belongs.
        /// </summary>
        IClusterGroup ClusterGroup { get; }

        /// <summary>
        /// Sends a message with specified topic to the nodes in the underlying cluster group.
        /// </summary>
        /// <param name="message">Message to send.</param>
        /// <param name="topic">Topic to send to, null for default topic.</param>
        void Send(object message, object topic = null);

        /// <summary>
        /// Sends messages with specified topic to the nodes in the underlying cluster group.
        /// </summary>
        /// <param name="messages">Messages to send.</param>
        /// <param name="topic">Topic to send to, null for default topic.</param>
        void Send(IEnumerable messages, object topic = null);

        /// <summary>
        /// Sends a message with specified topic to the nodes in the underlying cluster group.
        /// Messages sent with this method will arrive in the same order they were sent. Note that if a topic is used
        /// for ordered messages, then it cannot be reused for non-ordered messages.
        /// </summary>
        /// <param name="message">Message to send.</param>
        /// <param name="topic">Topic to send to, null for default topic.</param>
        /// <param name="timeout">
        /// Message timeout, null for for default value from configuration (IgniteConfiguration.getNetworkTimeout).
        /// </param>
        void SendOrdered(object message, object topic = null, TimeSpan? timeout = null);

        /// <summary>
        /// Adds local listener for given topic on local node only. This listener will be notified whenever any
        /// node within the cluster group will send a message for a given topic to this node. Local listen
        /// subscription will happen regardless of whether local node belongs to this cluster group or not.
        /// </summary>
        /// <param name="filter">
        /// Predicate that is called on each received message. If predicate returns false,
        /// then it will be unsubscribed from any further notifications.
        /// </param>
        /// <param name="topic">Topic to subscribe to.</param>
        void LocalListen<T>(IMessageFilter<T> filter, object topic = null);

        /// <summary>
        /// Unregisters local listener for given topic on local node only.
        /// </summary>
        /// <param name="filter">Listener predicate.</param>
        /// <param name="topic">Topic to unsubscribe from.</param>
        void StopLocalListen<T>(IMessageFilter<T> filter, object topic = null);

        /// <summary>
        /// Adds a message listener for a given topic to all nodes in the cluster group (possibly including
        /// this node if it belongs to the cluster group as well). This means that any node within this cluster
        /// group can send a message for a given topic and all nodes within the cluster group will receive
        /// listener notifications.
        /// </summary>
        /// <param name="filter">Listener predicate.</param>
        /// <param name="topic">Topic to unsubscribe from.</param>
        /// <returns>
        /// Operation ID that can be passed to <see cref="StopRemoteListen"/> method to stop listening.
        /// </returns>
        [AsyncSupported]
        Guid RemoteListen<T>(IMessageFilter<T> filter, object topic = null);

        /// <summary>
        /// Unregisters all listeners identified with provided operation ID on all nodes in the cluster group.
        /// </summary>
        /// <param name="opId">Operation ID that was returned from <see cref="RemoteListen{T}"/> method.</param>
        [AsyncSupported]
        void StopRemoteListen(Guid opId);
    }
}