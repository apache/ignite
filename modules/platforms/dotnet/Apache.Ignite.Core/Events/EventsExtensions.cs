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
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Events;
    using A = Apache.Ignite.Core.Impl.Common.IgniteArgumentCheck;

    /// <summary>
    /// Extension methods for <see cref="IEvents"/>.
    /// </summary>
    public static class EventsExtensions
    {
        /// <summary>
        /// Adds an event listener for local events. Note that listener will be added regardless of whether
        /// local node is in this cluster group or not.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="events">Events instance.</param>
        /// <param name="listener">Predicate that is called on each received event. If predicate returns false,
        /// it will be unregistered and will stop receiving events.</param>
        /// <param name="types">Event types for which this listener will be notified, should not be empty.</param>
        /// <returns>
        /// Event filter to be used in <see cref="IEvents.StopLocalListen{T}(IEventListener{T}, int[])"/>.
        /// </returns>
        public static IEventListener<T> LocalListen<T>(this IEvents events, Func<T, bool> listener, params int[] types)
            where T : IEvent
        {
            A.NotNull(events, "events");
            A.NotNull(listener, "listener");

            var listener0 = new EventDelegateFilter<T>(listener);

            events.LocalListen(listener0, types);

            return listener0;
        }

        /// <summary>
        /// Adds an event listener for local events. Note that listener will be added regardless of whether
        /// local node is in this cluster group or not.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="events">Events instance.</param>
        /// <param name="listener">Predicate that is called on each received event. If predicate returns false,
        /// it will be unregistered and will stop receiving events.</param>
        /// <param name="types">Event types for which this listener will be notified, should not be empty.</param>
        /// <returns>
        /// Event filter to be used in <see cref="IEvents.StopLocalListen{T}(IEventListener{T}, int[])"/>.
        /// </returns>
        public static IEventListener<T> LocalListen<T>(this IEvents events, Func<T, bool> listener, 
            IEnumerable<int> types)
            where T : IEvent
        {
            A.NotNull(events, "events");
            A.NotNull(listener, "listener");

            var listener0 = new EventDelegateFilter<T>(listener);

            events.LocalListen(listener0, types);

            return listener0;
        }

        /// <summary> 
        /// Waits for the specified events. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param> 
        /// <param name="types">Types of the events to wait for.  
        /// If not provided, all events will be passed to the filter.</param> 
        /// <returns>Grid event.</returns> 
        public static T WaitForLocal<T>(this IEvents events, Func<T, bool> filter, params int[] types)
            where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.WaitForLocal(filter0, types);
        }
        
        /// <summary> 
        /// Waits for the specified events. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param> 
        /// <param name="types">Types of the events to wait for.  
        /// If not provided, all events will be passed to the filter.</param> 
        /// <returns>Grid event.</returns> 
        public static T WaitForLocal<T>(this IEvents events, Func<T, bool> filter, IEnumerable<int> types)
            where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.WaitForLocal(filter0, types);
        }
        
        /// <summary> 
        /// Waits for the specified events. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param> 
        /// <param name="types">Types of the events to wait for.  
        /// If not provided, all events will be passed to the filter.</param> 
        /// <returns>Grid event.</returns> 
        public static Task<T> WaitForLocalAsync<T>(this IEvents events, Func<T, bool> filter, params int[] types)
            where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.WaitForLocalAsync(filter0, types);
        }
        
        /// <summary> 
        /// Waits for the specified events. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param> 
        /// <param name="types">Types of the events to wait for.  
        /// If not provided, all events will be passed to the filter.</param> 
        /// <returns>Grid event.</returns> 
        public static Task<T> WaitForLocalAsync<T>(this IEvents events, Func<T, bool> filter, IEnumerable<int> types)
            where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.WaitForLocalAsync(filter0, types);
        }
        
        /// <summary> 
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param> 
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param> 
        /// <param name="types">Event types to be queried.</param> 
        /// <returns>Collection of grid events returned from specified nodes.</returns> 
        public static ICollection<T> RemoteQuery<T>(this IEvents events, Func<T, bool> filter, TimeSpan? timeout = null,
            params int[] types) where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.RemoteQuery(filter0, timeout, types);
        }

        /// <summary> 
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param> 
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param> 
        /// <param name="types">Event types to be queried.</param> 
        /// <returns>Collection of grid events returned from specified nodes.</returns> 
        public static ICollection<T> RemoteQuery<T>(this IEvents events, Func<T, bool> filter, TimeSpan? timeout = null,
            IEnumerable<int> types = null) where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.RemoteQuery(filter0, timeout, types);
        }

        /// <summary> 
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param> 
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param> 
        /// <param name="types">Event types to be queried.</param> 
        /// <returns>Collection of grid events returned from specified nodes.</returns> 
        public static Task<ICollection<T>> RemoteQueryAsync<T>(this IEvents events, Func<T, bool> filter, 
            TimeSpan? timeout = null, params int[] types) where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.RemoteQueryAsync(filter0, timeout, types);
        }

        /// <summary> 
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection. 
        /// </summary>
        /// <param name="events">Events instance.</param> 
        /// <typeparam name="T">Type of events.</typeparam> 
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param> 
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param> 
        /// <param name="types">Event types to be queried.</param> 
        /// <returns>Collection of grid events returned from specified nodes.</returns> 
        public static Task<ICollection<T>> RemoteQueryAsync<T>(this IEvents events, Func<T, bool> filter, 
            TimeSpan? timeout = null, IEnumerable<int> types = null) where T : IEvent
        {
            A.NotNull(events, "events");

            var filter0 = filter == null ? null : new EventDelegateFilter<T>(filter);

            return events.RemoteQueryAsync(filter0, timeout, types);
        }
    }
}
