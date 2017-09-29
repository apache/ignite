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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;

    /// <summary>
    /// Ignite events.
    /// </summary>
    internal sealed class Events : PlatformTargetAdapter, IEvents
    {
        /// <summary>
        /// Opcodes.
        /// </summary>
        private enum Op
        {
            RemoteQuery = 1,
            RemoteListen = 2,
            StopRemoteListen = 3,
            WaitForLocal = 4,
            LocalQuery = 5,
            // ReSharper disable once UnusedMember.Local
            RecordLocal = 6,
            EnableLocal = 8,
            DisableLocal = 9,
            GetEnabledEvents = 10,
            IsEnabled = 12,
            LocalListen = 13,
            StopLocalListen = 14,
            RemoteQueryAsync = 15,
            WaitForLocalAsync = 16
        }

        /** Map from user func to local wrapper, needed for invoke/unsubscribe. */
        private readonly Dictionary<object, Dictionary<int, LocalHandledEventFilter>> _localFilters
            = new Dictionary<object, Dictionary<int, LocalHandledEventFilter>>();

        /** Cluster group. */
        private readonly IClusterGroup _clusterGroup;

        /// <summary>
        /// Initializes a new instance of the <see cref="Events" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        public Events(IPlatformTargetInternal target, IClusterGroup clusterGroup) 
            : base(target)
        {
            Debug.Assert(clusterGroup != null);

            _clusterGroup = clusterGroup;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _clusterGroup; }
        }

        /** */
        private Ignite Ignite
        {
            get { return (Ignite) ClusterGroup.Ignite; }
        }

        /** <inheritDoc /> */
        public ICollection<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, params int[] types)
            where T : IEvent
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            return DoOutInOp((int) Op.RemoteQuery,
                writer => WriteRemoteQuery(filter, timeout, types, writer),
                reader => ReadEvents<T>(reader));
        }

        /** <inheritDoc /> */
        public Task<ICollection<T>> RemoteQueryAsync<T>(IEventFilter<T> filter, TimeSpan? timeout = null, 
            params int[] types) where T : IEvent
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            // ReSharper disable once RedundantTypeArgumentsOfMethod (won't compile in VS2010)
            return DoOutOpAsync<ICollection<T>>((int) Op.RemoteQueryAsync,
                w => WriteRemoteQuery(filter, timeout, types, w), convertFunc: ReadEvents<T>);
        }

        /** <inheritDoc /> */
        public ICollection<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, 
            IEnumerable<int> types = null) where T : IEvent
        {
            return RemoteQuery(filter, timeout, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public Task<ICollection<T>> RemoteQueryAsync<T>(IEventFilter<T> filter, TimeSpan? timeout = null, 
            IEnumerable<int> types = null) where T : IEvent
        {
            return RemoteQueryAsync(filter, timeout, TypesToArray(types));
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public Guid? RemoteListen<T>(int bufSize = 1, TimeSpan? interval = null, bool autoUnsubscribe = true,
            IEventFilter<T> localListener = null, IEventFilter<T> remoteFilter = null, params int[] types)
            where T : IEvent
        {
            IgniteArgumentCheck.Ensure(bufSize > 0, "bufSize", "should be > 0");
            IgniteArgumentCheck.Ensure(interval == null || interval.Value.TotalMilliseconds > 0, "interval", "should be null or >= 0");

            return DoOutInOp((int) Op.RemoteListen,
                writer =>
                {
                    writer.WriteInt(bufSize);
                    writer.WriteLong((long) (interval == null ? 0 : interval.Value.TotalMilliseconds));
                    writer.WriteBoolean(autoUnsubscribe);

                    writer.WriteBoolean(localListener != null);

                    if (localListener != null)
                    {
                        var listener = new RemoteListenEventFilter(Ignite, e => localListener.Invoke((T) e));
                        writer.WriteLong(Ignite.HandleRegistry.Allocate(listener));
                    }

                    writer.WriteBoolean(remoteFilter != null);

                    if (remoteFilter != null)
                        writer.Write(remoteFilter);

                    WriteEventTypes(types, writer);
                },
                reader => Marshaller.StartUnmarshal(reader).ReadGuid());
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public Guid? RemoteListen<T>(int bufSize = 1, TimeSpan? interval = null, bool autoUnsubscribe = true,
            IEventFilter<T> localListener = null, IEventFilter<T> remoteFilter = null, IEnumerable<int> types = null)
            where T : IEvent
        {
            return RemoteListen(bufSize, interval, autoUnsubscribe, localListener, remoteFilter, TypesToArray(types));
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public void StopRemoteListen(Guid opId)
        {
            DoOutOp((int) Op.StopRemoteListen, writer =>
            {
                Marshaller.StartMarshal(writer).WriteGuid(opId);
            });
        }

        /** <inheritDoc /> */
        public IEvent WaitForLocal(params int[] types)
        {
            return WaitForLocal<IEvent>(null, types);
        }

        /** <inheritDoc /> */
        public Task<IEvent> WaitForLocalAsync(params int[] types)
        {
            return WaitForLocalAsync<IEvent>(null, types);
        }

        /** <inheritDoc /> */
        public IEvent WaitForLocal(IEnumerable<int> types)
        {
            return WaitForLocal(TypesToArray(types));
        }

        /** <inheritDoc /> */
        public Task<IEvent> WaitForLocalAsync(IEnumerable<int> types)
        {
            return WaitForLocalAsync<IEvent>(null, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public T WaitForLocal<T>(IEventFilter<T> filter, params int[] types) where T : IEvent
        {
            var hnd = GetFilterHandle(filter);

            try
            {
                return DoOutInOp((int) Op.WaitForLocal,
                    writer =>
                    {
                        writer.WriteObject(hnd);
                        WriteEventTypes(types, writer);
                    },
                    reader => EventReader.Read<T>(Marshaller.StartUnmarshal(reader)));
            }
            finally
            {
                if (hnd != null)
                    Ignite.HandleRegistry.Release(hnd.Value);
            }
        }

        /** <inheritDoc /> */
        public Task<T> WaitForLocalAsync<T>(IEventFilter<T> filter, params int[] types) where T : IEvent
        {
            var hnd = GetFilterHandle(filter);

            try
            {
                var task = DoOutOpAsync((int) Op.WaitForLocalAsync, writer =>
                {
                    writer.WriteObject(hnd);
                    WriteEventTypes(types, writer);
                }, convertFunc: EventReader.Read<T>);

                if (hnd != null)
                {
                    // Dispose handle as soon as future ends.
                    task.ContinueWith(x => Ignite.HandleRegistry.Release(hnd.Value));
                }

                return task;
            }
            catch (Exception)
            {
                if (hnd != null)
                    Ignite.HandleRegistry.Release(hnd.Value);

                throw;
            }
        }

        /** <inheritDoc /> */
        public T WaitForLocal<T>(IEventFilter<T> filter, IEnumerable<int> types) where T : IEvent
        {
            return WaitForLocal(filter, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public Task<T> WaitForLocalAsync<T>(IEventFilter<T> filter, IEnumerable<int> types) where T : IEvent
        {
            return WaitForLocalAsync(filter, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public ICollection<IEvent> LocalQuery(params int[] types)
        {
            return DoOutInOp((int) Op.LocalQuery,
                writer => WriteEventTypes(types, writer),
                reader => ReadEvents<IEvent>(reader));
        }

        /** <inheritDoc /> */
        public ICollection<IEvent> LocalQuery(IEnumerable<int> types)
        {
            return LocalQuery(TypesToArray(types));
        }

        /** <inheritDoc /> */
        public void RecordLocal(IEvent evt)
        {
            throw new NotImplementedException("IGNITE-1410");
        }

        /** <inheritDoc /> */
        public void LocalListen<T>(IEventListener<T> listener, params int[] types) where T : IEvent
        {
            IgniteArgumentCheck.NotNull(listener, "listener");
            IgniteArgumentCheck.NotNullOrEmpty(types, "types");

            foreach (var type in types)
                LocalListen(listener, type);
        }

        /** <inheritDoc /> */
        public void LocalListen<T>(IEventListener<T> listener, IEnumerable<int> types) where T : IEvent
        {
            LocalListen(listener, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public bool StopLocalListen<T>(IEventListener<T> listener, params int[] types) where T : IEvent
        {
            lock (_localFilters)
            {
                Dictionary<int, LocalHandledEventFilter> filters;

                if (_localFilters.TryGetValue(listener, out filters))
                {
                    var success = false;

                    // Should do this inside lock to avoid race with subscription
                    // ToArray is required because we are going to modify underlying dictionary during enumeration
                    foreach (var filter in GetLocalFilters(listener, types).ToArray())
                        success |= (DoOutInOp((int) Op.StopLocalListen, filter.Handle) == True);

                    return success;
                }
            }

            // Looks for a predefined filter (IgniteConfiguration.LocalEventListeners).
            var ids = Ignite.Configuration.LocalEventListenerIds;

            int predefinedListenerId;
            if (ids != null && ids.TryGetValue(listener, out predefinedListenerId))
            {
                return DoOutInOp((int) Op.StopLocalListen, w =>
                {
                    w.WriteInt(predefinedListenerId);
                    w.WriteIntArray(types);
                }, s => s.ReadBool());
            }

            return false;
        }

        /** <inheritDoc /> */
        public bool StopLocalListen<T>(IEventListener<T> listener, IEnumerable<int> types) where T : IEvent
        {
            return StopLocalListen(listener, TypesToArray(types));
        }

        /** <inheritDoc /> */
        public void EnableLocal(IEnumerable<int> types)
        {
            EnableLocal(TypesToArray(types));
        }

        /** <inheritDoc /> */
        public void EnableLocal(params int[] types)
        {
            IgniteArgumentCheck.NotNullOrEmpty(types, "types");

            DoOutOp((int)Op.EnableLocal, writer => WriteEventTypes(types, writer));
        }

        /** <inheritDoc /> */
        public void DisableLocal(params int[] types)
        {
            IgniteArgumentCheck.NotNullOrEmpty(types, "types");

            DoOutOp((int)Op.DisableLocal, writer => WriteEventTypes(types, writer));
        }

        /** <inheritDoc /> */
        public void DisableLocal(IEnumerable<int> types)
        {
            DisableLocal(TypesToArray(types));
        }

        /** <inheritDoc /> */
        public ICollection<int> GetEnabledEvents()
        {
            return DoInOp((int)Op.GetEnabledEvents, reader => ReadEventTypes(reader));
        }

        /** <inheritDoc /> */
        public bool IsEnabled(int type)
        {
            return DoOutInOp((int) Op.IsEnabled, type) == True;
        }

        /// <summary>
        /// Gets the filter handle.
        /// </summary>
        private long? GetFilterHandle<T>(IEventFilter<T> filter) where T : IEvent
        {
            return filter != null 
                ? Ignite.HandleRegistry.Allocate(new LocalEventFilter<T>(Marshaller, filter)) 
                : (long?) null;
        }

        /// <summary>
        /// Reads events from a binary stream.
        /// </summary>
        /// <typeparam name="T">Event type.</typeparam>
        /// <param name="reader">Reader.</param>
        /// <returns>Resulting list or null.</returns>
        private ICollection<T> ReadEvents<T>(IBinaryStream reader) where T : IEvent
        {
            return ReadEvents<T>(Marshaller.StartUnmarshal(reader));
        }

        /// <summary>
        /// Reads events from a binary reader.
        /// </summary>
        /// <typeparam name="T">Event type.</typeparam>
        /// <param name="binaryReader">Reader.</param>
        /// <returns>Resulting list or null.</returns>
        private static ICollection<T> ReadEvents<T>(BinaryReader binaryReader) where T : IEvent
        {
            var count = binaryReader.GetRawReader().ReadInt();

            if (count == -1)
                return null;

            var result = new List<T>(count);

            for (var i = 0; i < count; i++)
                result.Add(EventReader.Read<T>(binaryReader));

            return result;
        }

        /// <summary>
        /// Gets local filters by user listener and event type.
        /// </summary>
        /// <param name="listener">Listener.</param>
        /// <param name="types">Types.</param>
        /// <returns>Collection of local listener wrappers.</returns>
        [SuppressMessage("ReSharper", "InconsistentlySynchronizedField",
            Justification = "This private method should be always called within a lock on localFilters")]
        private IEnumerable<LocalHandledEventFilter> GetLocalFilters(object listener, int[] types)
        {
            Dictionary<int, LocalHandledEventFilter> filters;

            if (!_localFilters.TryGetValue(listener, out filters))
                return Enumerable.Empty<LocalHandledEventFilter>();

            if (types.Length == 0)
                return filters.Values;

            return types.Select(type =>
            {
                LocalHandledEventFilter filter;

                return filters.TryGetValue(type, out filter) ? filter : null;
            }).Where(x => x != null);
        }

        /// <summary>
        /// Adds an event listener for local events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Predicate that is called on each received event.</param>
        /// <param name="type">Event type for which this listener will be notified</param>
        private void LocalListen<T>(IEventListener<T> listener, int type) where T : IEvent
        {
            lock (_localFilters)
            {
                Dictionary<int, LocalHandledEventFilter> filters;

                if (!_localFilters.TryGetValue(listener, out filters))
                {
                    filters = new Dictionary<int, LocalHandledEventFilter>();

                    _localFilters[listener] = filters;
                }

                LocalHandledEventFilter localFilter;

                if (!filters.TryGetValue(type, out localFilter))
                {
                    localFilter = CreateLocalListener(listener, type);

                    filters[type] = localFilter;
                }

                DoOutOp((int) Op.LocalListen, (IBinaryStream s) =>
                {
                    s.WriteLong(localFilter.Handle);
                    s.WriteInt(type);
                });
            }
        }

        /// <summary>
        /// Creates a user filter wrapper.
        /// </summary>
        /// <typeparam name="T">Event object type.</typeparam>
        /// <param name="listener">Listener.</param>
        /// <param name="type">Event type.</param>
        /// <returns>Created wrapper.</returns>
        private LocalHandledEventFilter CreateLocalListener<T>(IEventListener<T> listener, int type) where T : IEvent
        {
            var result = new LocalHandledEventFilter(
                stream => InvokeLocalListener(stream, listener),
                unused =>
                {
                    lock (_localFilters)
                    {
                        Dictionary<int, LocalHandledEventFilter> filters;

                        if (_localFilters.TryGetValue(listener, out filters))
                        {
                            filters.Remove(type);

                            if (filters.Count == 0)
                                _localFilters.Remove(listener);
                        }
                    }
                });

            result.Handle = Ignite.HandleRegistry.Allocate(result);

            return result;
        }

        /// <summary>
        /// Invokes local filter using data from specified stream.
        /// </summary>
        /// <typeparam name="T">Event object type.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="listener">The listener.</param>
        /// <returns>Filter invocation result.</returns>
        private bool InvokeLocalListener<T>(IBinaryStream stream, IEventListener<T> listener) where T : IEvent
        {
            var evt = EventReader.Read<T>(Marshaller.StartUnmarshal(stream));

            return listener.Invoke(evt);
        }

        /// <summary>
        /// Writes the event types.
        /// </summary>
        /// <param name="types">Types.</param>
        /// <param name="writer">Writer.</param>
        private static void WriteEventTypes(int[] types, IBinaryRawWriter writer)
        {
            if (types != null && types.Length == 0)
                types = null;  // empty array means no type filtering

            writer.WriteIntArray(types);
        }

        /// <summary>
        /// Writes the event types.
        /// </summary>
        /// <param name="reader">Reader.</param>
        private int[] ReadEventTypes(IBinaryStream reader)
        {
            return Marshaller.StartUnmarshal(reader).ReadIntArray();
        }

        /// <summary>
        /// Converts types enumerable to array.
        /// </summary>
        private static int[] TypesToArray(IEnumerable<int> types)
        {
            if (types == null)
                return null;

            return types as int[] ?? types.ToArray();
        }

        /// <summary>
        /// Writes the remote query.
        /// </summary>
        /// <param name="filter">The filter.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="types">The types.</param>
        /// <param name="writer">The writer.</param>
        private static void WriteRemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout, int[] types, 
            IBinaryRawWriter writer)
            where T : IEvent
        {
            writer.WriteObject(filter);

            writer.WriteLong((long)(timeout == null ? 0 : timeout.Value.TotalMilliseconds));

            WriteEventTypes(types, writer);
        }

        /// <summary>
        /// Local user filter wrapper.
        /// </summary>
        private class LocalEventFilter<T> : IInteropCallback where T : IEvent
        {
            /** */
            private readonly Marshaller _marshaller;

            /** */
            private readonly IEventFilter<T> _listener;

            /// <summary>
            /// Initializes a new instance of the <see cref="LocalEventFilter{T}"/> class.
            /// </summary>
            public LocalEventFilter(Marshaller marshaller, IEventFilter<T> listener)
            {
                _marshaller = marshaller;
                _listener = listener;
            }

            /** <inheritdoc /> */
            public int Invoke(IBinaryStream stream)
            {
                var evt = EventReader.Read<T>(_marshaller.StartUnmarshal(stream));

                return _listener.Invoke(evt) ? 1 : 0;
            }
        }

        /// <summary>
        /// Local user filter wrapper with handle.
        /// </summary>
        private class LocalHandledEventFilter : Handle<Func<IBinaryStream, bool>>, IInteropCallback
        {
            /** */
            public long Handle;

            /** <inheritdoc /> */
            public int Invoke(IBinaryStream stream)
            {
                return Target(stream) ? 1 : 0;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="LocalHandledEventFilter"/> class.
            /// </summary>
            /// <param name="invokeFunc">The invoke function.</param>
            /// <param name="releaseAction">The release action.</param>
            public LocalHandledEventFilter(
                Func<IBinaryStream, bool> invokeFunc, Action<Func<IBinaryStream, bool>> releaseAction) 
                : base(invokeFunc, releaseAction)
            {
                // No-op.
            }
        }
    }
}