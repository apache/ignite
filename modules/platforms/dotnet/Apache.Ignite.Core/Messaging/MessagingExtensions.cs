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

namespace Apache.Ignite.Core.Messaging
{
    using System;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Messaging;
    using A = Apache.Ignite.Core.Impl.Common.IgniteArgumentCheck;

    /// <summary>
    /// Extension methods for <see cref="IMessaging"/>.
    /// </summary>
    public static class MessagingExtensions
    {
        /// <summary>
        /// Adds local listener for given topic on local node only. This listener will be notified whenever any
        /// node within the cluster group will send a message for a given topic to this node. Local listen
        /// subscription will happen regardless of whether local node belongs to this cluster group or not.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messaging">Messaging instance.</param>
        /// <param name="filter">Predicate that is called on each received message. If predicate returns false,
        /// then it will be unsubscribed from any further notifications.</param>
        /// <param name="topic">Topic to subscribe to.</param>
        /// <returns>
        /// Message filter that can be passed to <see cref="IMessaging.StopLocalListen{T}"/> method to stop listening.
        /// </returns>
        public static IMessageListener<T> LocalListen<T>(this IMessaging messaging, Func<Guid, T, bool> filter, 
            object topic = null)
        {
            A.NotNull(messaging, "messaging");
            A.NotNull(filter, "filter");

            var filter0 = new MessageDelegateFilter<T>(filter);

            messaging.LocalListen(filter0, topic);

            return filter0;
        }

        /// <summary>
        /// Adds a message listener for a given topic to all nodes in the cluster group (possibly including
        /// this node if it belongs to the cluster group as well). This means that any node within this cluster
        /// group can send a message for a given topic and all nodes within the cluster group will receive
        /// listener notifications.
        /// </summary>
        /// <param name="messaging">Messaging instance.</param>
        /// <param name="filter">Listener predicate.</param>
        /// <param name="topic">Topic to unsubscribe from.</param>
        /// <returns>
        /// Operation ID that can be passed to <see cref="IMessaging.StopRemoteListen"/> method to stop listening.
        /// </returns>
        public static Guid RemoteListen<T>(this IMessaging messaging, Func<Guid, T, bool> filter, object topic = null)
        {
            A.NotNull(messaging, "messaging");
            A.NotNull(filter, "filter");

            var filter0 = new MessageDelegateFilter<T>(filter);

            return messaging.RemoteListen(filter0, topic);
        }

        /// <summary>
        /// Adds a message listener for a given topic to all nodes in the cluster group (possibly including
        /// this node if it belongs to the cluster group as well). This means that any node within this cluster
        /// group can send a message for a given topic and all nodes within the cluster group will receive
        /// listener notifications.
        /// </summary>
        /// <param name="messaging">Messaging instance.</param>
        /// <param name="filter">Listener predicate.</param>
        /// <param name="topic">Topic to unsubscribe from.</param>
        /// <returns>
        /// Operation ID that can be passed to <see cref="IMessaging.StopRemoteListen"/> method to stop listening.
        /// </returns>
        public static Task<Guid> RemoteListenAsync<T>(this IMessaging messaging, Func<Guid, T, bool> filter, object topic = null)
        {
            A.NotNull(messaging, "messaging");
            A.NotNull(filter, "filter");

            var filter0 = new MessageDelegateFilter<T>(filter);

            return messaging.RemoteListenAsync(filter0, topic);
        }
    }
}
