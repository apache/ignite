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
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Provides functionality for event notifications on nodes defined by <see cref="ClusterGroup"/>.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IEvents
    {
        /// <summary>
        /// Gets the cluster group to which this instance belongs.
        /// </summary>
        IClusterGroup ClusterGroup { get; }

        /// <summary>
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param>
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param>
        /// <param name="types">Event types to be queried.</param>
        /// <returns>Collection of Ignite events returned from specified nodes.</returns>
        ICollection<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, params int[] types) 
            where T : IEvent;

        /// <summary>
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param>
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param>
        /// <param name="types">Event types to be queried.</param>
        /// <returns>Collection of Ignite events returned from specified nodes.</returns>
        Task<ICollection<T>> RemoteQueryAsync<T>(IEventFilter<T> filter, TimeSpan? timeout = null, params int[] types) 
            where T : IEvent;

        /// <summary>
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param>
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param>
        /// <param name="types">Event types to be queried.</param>
        /// <returns>Collection of Ignite events returned from specified nodes.</returns>
        ICollection<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, IEnumerable<int> types = null) 
            where T : IEvent;

        /// <summary>
        /// Queries nodes in this cluster group for events using passed in predicate filter for event selection.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Predicate filter used to query events on remote nodes.</param>
        /// <param name="timeout">Maximum time to wait for result, null or 0 to wait forever.</param>
        /// <param name="types">Event types to be queried.</param>
        /// <returns>Collection of Ignite events returned from specified nodes.</returns>
        Task<ICollection<T>> RemoteQueryAsync<T>(IEventFilter<T> filter, TimeSpan? timeout = null, IEnumerable<int> types = null) 
            where T : IEvent;

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        IEvent WaitForLocal(params int[] types);

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        Task<IEvent> WaitForLocalAsync(params int[] types);

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        IEvent WaitForLocal(IEnumerable<int> types);

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        Task<IEvent> WaitForLocalAsync(IEnumerable<int> types);

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        T WaitForLocal<T>(IEventFilter<T> filter, params int[] types) where T : IEvent;

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        Task<T> WaitForLocalAsync<T>(IEventFilter<T> filter, params int[] types) where T : IEvent;

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        T WaitForLocal<T>(IEventFilter<T> filter, IEnumerable<int> types) where T : IEvent;

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Ignite event.</returns>
        Task<T> WaitForLocalAsync<T>(IEventFilter<T> filter, IEnumerable<int> types) where T : IEvent;

        /// <summary>
        /// Queries local node for events using of specified types.
        /// </summary>
        /// <param name="types">Event types to be queried. Optional.</param>
        /// <returns>Collection of Ignite events found on local node.</returns>
        ICollection<IEvent> LocalQuery(params int[] types);

        /// <summary>
        /// Queries local node for events using of specified types.
        /// </summary>
        /// <param name="types">Event types to be queried. Optional.</param>
        /// <returns>Collection of Ignite events found on local node.</returns>
        ICollection<IEvent> LocalQuery(IEnumerable<int> types);

        /// <summary>
        /// Records customer user generated event. All registered local listeners will be notified.
        /// <para/>
        /// NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
        /// internal Ignite events and should not be used by user-defined events.
        /// Attempt to record internal event with this method will cause <see cref="ArgumentException"/> to be thrown.
        /// </summary>
        /// <param name="evt">Locally generated event.</param>
        /// <exception cref="ArgumentException">If event type is within Ignite reserved range (1 to 1000)</exception>
        void RecordLocal(IEvent evt);

        /// <summary>
        /// Adds an event listener for local events. Note that listener will be added regardless of whether 
        /// local node is in this cluster group or not.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Predicate that is called on each received event. If predicate returns false,
        /// it will be unregistered and will stop receiving events.</param>
        /// <param name="types">Event types for which this listener will be notified, should not be empty.</param>
        void LocalListen<T>(IEventListener<T> listener, params int[] types) where T : IEvent;

        /// <summary>
        /// Adds an event listener for local events. Note that listener will be added regardless of whether 
        /// local node is in this cluster group or not.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Predicate that is called on each received event. If predicate returns false,
        /// it will be unregistered and will stop receiving events.</param>
        /// <param name="types">Event types for which this listener will be notified, should not be empty.</param>
        void LocalListen<T>(IEventListener<T> listener, IEnumerable<int> types) where T : IEvent;

        /// <summary>
        /// Removes local event listener.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Local event listener to remove.</param>
        /// <param name="types">Types of events for which to remove listener. If not specified, then listener
        /// will be removed for all types it was registered for.</param>
        /// <returns>True if listener was removed, false otherwise.</returns>
        bool StopLocalListen<T>(IEventListener<T> listener, params int[] types) where T : IEvent;

        /// <summary>
        /// Removes local event listener.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Local event listener to remove.</param>
        /// <param name="types">Types of events for which to remove listener. If not specified, then listener
        /// will be removed for all types it was registered for.</param>
        /// <returns>True if listener was removed, false otherwise.</returns>
        bool StopLocalListen<T>(IEventListener<T> listener, IEnumerable<int> types) where T : IEvent;

        /// <summary>
        /// Enables provided events. Allows to start recording events that were disabled before. 
        /// Note that provided events will be enabled regardless of whether local node is in this cluster group or not.
        /// </summary>
        /// <param name="types">Events to enable.</param>
        void EnableLocal(params int[] types);

        /// <summary>
        /// Enables provided events. Allows to start recording events that were disabled before. 
        /// Note that provided events will be enabled regardless of whether local node is in this cluster group or not.
        /// </summary>
        /// <param name="types">Events to enable.</param>
        void EnableLocal(IEnumerable<int> types);

        /// <summary>
        /// Disables provided events. Allows to stop recording events that were enabled before. Note that specified 
        /// events will be disabled regardless of whether local node is in this cluster group or not.
        /// </summary>
        /// <param name="types">Events to disable.</param>
        void DisableLocal(params int[] types);

        /// <summary>
        /// Disables provided events. Allows to stop recording events that were enabled before. Note that specified 
        /// events will be disabled regardless of whether local node is in this cluster group or not.
        /// </summary>
        /// <param name="types">Events to disable.</param>
        void DisableLocal(IEnumerable<int> types);

        /// <summary>
        /// Gets types of enabled events.
        /// </summary>
        /// <returns>Types of enabled events.</returns>
        ICollection<int> GetEnabledEvents();

        /// <summary>
        /// Determines whether the specified event is enabled.
        /// </summary>
        /// <param name="type">Event type.</param>
        /// <returns>Value indicating whether the specified event is enabled.</returns>
        bool IsEnabled(int type);
    }
}